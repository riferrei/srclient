package srclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"

	"golang.org/x/sync/semaphore"
)

// ISchemaRegistryClient provides the
// definition of the operations that
// this Schema Registry client provides.
type ISchemaRegistryClient interface {
	GetSubjects() ([]string, error)

	GetLatestSchema(subject string, isKey bool) (*Schema, error)
	GetSchemaVersions(subject string, isKey bool) ([]int, error)

	GetSchemaByID(schemaID int) (*Schema, error)
	GetSchemaBySubject(subject string, isKey bool) (*Schema, error)
	GetSchemaByVersion(subject string, version int, isKey bool) (*Schema, error)

	CreateSchema(subject string, schema string, schemaType SchemaType, isKey bool, references ...Reference) (*Schema, error)

	SetCachingEnabled(value bool)
	SetCodecCreationEnabled(value bool)
}

// SchemaRegistryClient allows interactions with
// Schema Registry over HTTP. Applications using
// this client can retrieve data about schemas,
// which in turn can be used to serialize and
// deserialize data.
type SchemaRegistryClient struct {
	schemaRegistryURL        string
	credentials              *credentials
	httpClient               *http.Client
	cachingEnabled           bool
	cachingEnabledLock       sync.RWMutex
	codecCreationEnabled     bool
	codecCreationEnabledLock sync.RWMutex

	idSchemaCache     map[int]*Schema
	idSchemaCacheLock sync.RWMutex

	subjectSchemaCache     map[string]*Schema
	subjectSchemaCacheLock sync.RWMutex

	sem *semaphore.Weighted
}

const (
	schemaByID       = "/schemas/ids/%d"
	subjectVersions  = "/subjects/%s/versions"
	subjectByVersion = "/subjects/%s/versions/%s"
	subjects         = "/subjects"
	contentType      = "application/vnd.schemaregistry.v1+json"
)

// CreateSchemaRegistryClient creates a client that allows
// interactions with Schema Registry over HTTP. Applications
// using this client can retrieve data about schemas, which
// in turn can be used to serialize and deserialize records.
func CreateSchemaRegistryClient(schemaRegistryURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		schemaRegistryURL:    schemaRegistryURL,
		httpClient:           &http.Client{Timeout: 5 * time.Second},
		cachingEnabled:       true,
		codecCreationEnabled: true,
		idSchemaCache:        make(map[int]*Schema),
		subjectSchemaCache:   make(map[string]*Schema),
		sem:                  semaphore.NewWeighted(16),
	}
}

// CreateSchemaRegistryClientWithOptions exposes the ability to give custom options
func CreateSchemaRegistryClientWithOptions(schemaRegistryURL string, options ...Option) *SchemaRegistryClient {
	client := CreateSchemaRegistryClient(schemaRegistryURL)
	for _, option := range options {
		option(client)
	}
	return client
}

// GetSubjects returns a list of all subjects in the registry
func (client *SchemaRegistryClient) GetSubjects() ([]string, error) {
	resp, err := client.httpRequest("GET", subjects, nil)
	if err != nil {
		return nil, err
	}
	var allSubjects = []string{}
	err = json.Unmarshal(resp, &allSubjects)
	if err != nil {
		return nil, err
	}
	return allSubjects, nil
}

// GetLatestSchema gets the schema associated with the given subject.
// The schema returned contains the last version for that subject.
func (client *SchemaRegistryClient) GetLatestSchema(subject string, isKey bool) (*Schema, error) {
	return client.requestSchemaByVersion(subject, "latest", isKey)
}

// GetSchemaVersions returns a list of versions from a given subject.
func (client *SchemaRegistryClient) GetSchemaVersions(subject string, isKey bool) ([]int, error) {

	concreteSubject := getConcreteSubject(subject, isKey)
	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectVersions, concreteSubject), nil)
	if err != nil {
		return nil, err
	}

	var versions = []int{}
	err = json.Unmarshal(resp, &versions)
	if err != nil {
		return nil, err
	}

	return versions, nil
}

// GetSchemaByID gets the schema associated with the given id.
func (client *SchemaRegistryClient) GetSchemaByID(id int) (*Schema, error) {
	if schema, ok := client.getFromIDCache(id); ok {
		return schema, nil
	}
	return client.requestSchemaByID(id)
}

// GetSchemaBySubject gets the schema associated with the given subject.
func (client *SchemaRegistryClient) GetSchemaBySubject(subject string, isKey bool) (*Schema, error) {
	concreteSubject := getConcreteSubject(subject, isKey)
	if schema, ok := client.getFromSubjectCache(concreteSubject); ok {
		return schema, nil
	}
	return client.GetLatestSchema(subject, isKey)
}

// GetSchemaByVersion gets the schema associated with the given subject.
// The schema returned contains the version specified as a parameter.
func (client *SchemaRegistryClient) GetSchemaByVersion(subject, version string, isKey bool) (*Schema, error) {
	concreteSubject := getConcreteSubject(subject, isKey)
	cacheKey := cacheKey(concreteSubject, version)
	if schema, ok := client.getFromVersionCache(cacheKey); ok {
		return schema, nil
	}
	return client.requestSchemaByVersion(subject, version, isKey)
}

// CreateSchema creates a new schema in Schema Registry and associates
// with the subject provided. It returns the newly created schema with
// all its associated information.
func (client *SchemaRegistryClient) CreateSchema(subject string, schema string, schemaType SchemaType, isKey bool, references ...Reference) (*Schema, error) {
	concreteSubject := getConcreteSubject(subject, isKey)

	switch schemaType {
	case Avro, Json:
		compiledRegex := regexp.MustCompile(`\r?\n`)
		schema = compiledRegex.ReplaceAllString(schema, " ")
	case Protobuf:
		break
	default:
		return nil, fmt.Errorf("invalid schema type. valid values are Avro, Json, or Protobuf")
	}

	if references == nil {
		references = make([]Reference, 0)
	}

	schemaReq := schemaRequest{Schema: schema, SchemaType: schemaType.String(), References: references}
	schemaBytes, err := json.Marshal(schemaReq)
	if err != nil {
		return nil, err
	}

	payload := bytes.NewBuffer(schemaBytes)
	resp, err := client.httpRequest("POST", fmt.Sprintf(subjectVersions, concreteSubject), payload)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, createError(strings.NewReader(err.Error()))
	}

	// Conceptually, the schema returned below will be the
	// exactly same one created above. However, since Schema
	// Registry can have multiple concurrent clients writing
	// schemas, this may produce an incorrect result. Thus,
	// this logic strongly relies on the idempotent guarantees
	// from Schema Registry, as well as in the best practice
	// that schemas don't change very often.
	return client.GetLatestSchema(subject, isKey)
}

// SetCachingEnabled allows the client to cache any values
// that have been returned, which may speed up performance
// if these values rarely changes.
func (client *SchemaRegistryClient) SetCachingEnabled(value bool) {
	client.cachingEnabledLock.Lock()
	defer client.cachingEnabledLock.Unlock()
	client.cachingEnabled = value
}

// CodecCreationEnabled allows the application to enable/disable
// the automatic creation of codec's when schemas are returned.
func (client *SchemaRegistryClient) SetCodecCreationEnabled(value bool) {
	client.codecCreationEnabledLock.Lock()
	defer client.codecCreationEnabledLock.Unlock()
	client.codecCreationEnabled = value
}

func (client *SchemaRegistryClient) requestSchemaByID(id int) (*Schema, error) {
	uri := fmt.Sprintf(schemaByID, id)
	resp, err := client.httpRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	schema, err := client.schemaFromResponse(resp)
	if err != nil {
		return nil, err
	}

	client.cacheByID(schema)
	return schema, nil
}

func (client *SchemaRegistryClient) requestSchemaByVersion(subject, version string, isKey bool) (*Schema, error) {
	concreteSubject := getConcreteSubject(subject, isKey)
	uri := fmt.Sprintf(subjectByVersion, concreteSubject, version)

	resp, err := client.httpRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	schema, err := client.schemaFromResponse(resp)
	if err != nil {
		return nil, err
	}

	client.cacheByVersion(concreteSubject, schema)

	return schema, nil
}

func (client *SchemaRegistryClient) schemaFromResponse(resp []byte) (*Schema, error) {
	schemaResp := new(schemaResponse)
	err := json.Unmarshal(resp, schemaResp)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if client.isCodecCreationEnabled() {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}

	return &Schema{
		id:      schemaResp.ID,
		schema:  schemaResp.Schema,
		version: schemaResp.Version,
		codec:   codec,
	}, nil
}

func (client *SchemaRegistryClient) httpRequest(method, uri string, payload io.Reader) ([]byte, error) {
	client.sem.Acquire(context.Background(), 1)
	defer client.sem.Release(1)

	url := fmt.Sprintf("%s%s", client.schemaRegistryURL, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	if client.credentials != nil {
		req.SetBasicAuth(client.credentials.username, client.credentials.password)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, createError(resp.Body)
	}

	return ioutil.ReadAll(resp.Body)
}

func (client *SchemaRegistryClient) isCachingEnabled() bool {
	client.cachingEnabledLock.RLock()
	defer client.cachingEnabledLock.RUnlock()
	return client.cachingEnabled
}

func (client *SchemaRegistryClient) isCodecCreationEnabled() bool {
	client.codecCreationEnabledLock.RLock()
	defer client.codecCreationEnabledLock.RUnlock()
	return client.codecCreationEnabled
}

func (client *SchemaRegistryClient) getFromIDCache(id int) (*Schema, bool) {
	if !client.isCachingEnabled() {
		return nil, false
	}
	client.idSchemaCacheLock.RLock()
	defer client.idSchemaCacheLock.RUnlock()
	val, exists := client.idSchemaCache[id]
	return val, exists
}

func (client *SchemaRegistryClient) getFromSubjectCache(concreteSubject string) (*Schema, bool) {
	cacheKeyByLatest := cacheKey(concreteSubject, "latest")
	return client.getFromVersionCache(cacheKeyByLatest)
}

func (client *SchemaRegistryClient) getFromVersionCache(cacheKey string) (*Schema, bool) {
	if !client.isCachingEnabled() {
		return nil, false
	}
	client.subjectSchemaCacheLock.RLock()
	defer client.subjectSchemaCacheLock.RUnlock()
	val, exists := client.subjectSchemaCache[cacheKey]
	return val, exists
}

func (client *SchemaRegistryClient) cacheByVersion(concreteSubject string, schema *Schema) {
	if !client.isCachingEnabled() {
		return
	}

	defer client.cacheByID(schema)

	cacheKeyByLatest := cacheKey(concreteSubject, "latest")
	cacheKeyByVersion := cacheKey(concreteSubject, strconv.Itoa(schema.Version()))

	client.subjectSchemaCacheLock.Lock()
	defer client.subjectSchemaCacheLock.Unlock()
	latestSchema, ok := client.subjectSchemaCache[cacheKeyByLatest]
	if !ok {
		client.subjectSchemaCache[cacheKeyByLatest] = schema
	} else if latestSchema.Version() < schema.Version() {
		client.subjectSchemaCache[cacheKeyByLatest] = schema
	}
	client.subjectSchemaCache[cacheKeyByVersion] = schema
}

func (client *SchemaRegistryClient) cacheByID(schema *Schema) {
	if !client.isCachingEnabled() {
		return
	}
	client.idSchemaCacheLock.Lock()
	defer client.idSchemaCacheLock.Unlock()
	client.idSchemaCache[schema.ID()] = schema
}

func cacheKey(subject string, version string) string {
	return fmt.Sprintf("%s-%s", subject, version)
}

func getConcreteSubject(subject string, isKey bool) string {
	if isKey {
		subject = fmt.Sprintf("%s-key", subject)
	} else {
		subject = fmt.Sprintf("%s-value", subject)
	}
	return subject
}

func createError(r io.Reader) error {
	decoder := json.NewDecoder(r)
	var errorResp struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}
	err := decoder.Decode(&errorResp)
	if err == nil {
		return fmt.Errorf("%d: %s", errorResp.ErrorCode, errorResp.Message)
	}
	return fmt.Errorf("error outside of error format: %w", err)
}
