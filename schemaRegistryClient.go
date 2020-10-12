package srclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

// ISchemaRegistryClient provides the
// definition of the operations that
// this Schema Registry client provides.
type ISchemaRegistryClient interface {
	GetSubjects() ([]string, error)
	GetSchema(schemaID int) (*Schema, error)
	GetLatestSchema(subject string, isKey bool) (*Schema, error)
	GetSchemaVersions(subject string, isKey bool) ([]int, error)
	GetSchemaByVersion(subject string, version int, isKey bool) (*Schema, error)
	CreateSchema(subject string, schema string, schemaType SchemaType, isKey bool, references ...Reference) (*Schema, error)
	SetCredentials(username string, password string)
	SetTimeout(timeout time.Duration)
	CachingEnabled(value bool)
	CodecCreationEnabled(value bool)
}

// SchemaRegistryClient allows interactions with
// Schema Registry over HTTP. Applications using
// this client can retrieve data about schemas,
// which in turn can be used to serialize and
// deserialize data.
type SchemaRegistryClient struct {
	schemaRegistryURL      string
	credentials            *credentials
	httpClient             *http.Client
	cachingEnabled         bool
	codecCreationEnabled   bool
	idSchemaCache          map[int]*Schema
	idSchemaCacheLock      sync.RWMutex
	subjectSchemaCache     map[string]*Schema
	subjectSchemaCacheLock sync.RWMutex
}

type SchemaType string

const (
	Protobuf SchemaType = "PROTOBUF"
	Avro     SchemaType = "AVRO"
	Json     SchemaType = "JSON"
)

func (s SchemaType) String() string {
	return string(s)
}

// Schema references use the import statement of Protobuf and
// the $ref field of JSON Schema. They are defined by the name
// of the import or $ref and the associated subject in the registry.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Schema is a data structure that holds all
// the relevant information about schemas.
type Schema struct {
	id      int
	schema  string
	version int
	codec   *goavro.Codec
}

type credentials struct {
	username string
	password string
}

type schemaRequest struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"`
	References []Reference `json:"references"`
}

type schemaResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	ID      int    `json:"id"`
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
	return &SchemaRegistryClient{schemaRegistryURL: schemaRegistryURL,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
		cachingEnabled: true, codecCreationEnabled: true,
		idSchemaCache:      make(map[int]*Schema),
		subjectSchemaCache: make(map[string]*Schema)}
}

// GetSchema gets the schema associated with the given id.
func (client *SchemaRegistryClient) GetSchema(schemaID int) (*Schema, error) {

	if client.cachingEnabled {
		client.idSchemaCacheLock.RLock()
		cachedSchema := client.idSchemaCache[schemaID]
		client.idSchemaCacheLock.RUnlock()
		if cachedSchema != nil {
			return cachedSchema, nil
		}
	}

	resp, err := client.httpRequest("GET", fmt.Sprintf(schemaByID, schemaID), nil)
	if err != nil {
		return nil, err
	}

	var schemaResp = new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}
	var codec *goavro.Codec
	if client.codecCreationEnabled {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		id:     schemaID,
		schema: schemaResp.Schema,
		codec:  codec,
	}

	if client.cachingEnabled {
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[schemaID] = schema
		client.idSchemaCacheLock.Unlock()
	}

	return schema, nil
}

// GetLatestSchema gets the schema associated with the given subject.
// The schema returned contains the last version for that subject.
func (client *SchemaRegistryClient) GetLatestSchema(subject string, isKey bool) (*Schema, error) {

	// In order to ensure consistency, we need
	// to temporarily disable caching to force
	// the retrieval of the latest release from
	// Schema Registry.
	cachingEnabled := client.cachingEnabled
	client.CachingEnabled(false)
	schema, err := client.getVersion(subject, "latest", isKey)
	client.CachingEnabled(cachingEnabled)

	return schema, err
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

// GetSchemaByVersion gets the schema associated with the given subject.
// The schema returned contains the version specified as a parameter.
func (client *SchemaRegistryClient) GetSchemaByVersion(subject string, version int, isKey bool) (*Schema, error) {
	return client.getVersion(subject, strconv.Itoa(version), isKey)
}

// CreateSchema creates a new schema in Schema Registry and associates
// with the subject provided. It returns the newly created schema with
// all its associated information.
func (client *SchemaRegistryClient) CreateSchema(subject string, schema string,
	schemaType SchemaType, isKey bool, references ...Reference) (*Schema, error) {

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
		return nil, err
	}
	// Conceptually, the schema returned below will be the
	// exactly same one created above. However, since Schema
	// Registry can have multiple concurrent clients writing
	// schemas, this may produce an incorrect result. Thus,
	// this logic strongly relies on the idempotent guarantees
	// from Schema Registry, as well as in the best practice
	// that schemas don't change very often.
	newSchema, err := client.GetLatestSchema(subject, isKey)
	if err != nil {
		return nil, err
	}

	if client.cachingEnabled {

		// Update the subject-2-schema cache
		cacheKey := cacheKey(concreteSubject,
			strconv.Itoa(newSchema.version))
		client.subjectSchemaCacheLock.Lock()
		client.subjectSchemaCache[cacheKey] = newSchema
		client.subjectSchemaCacheLock.Unlock()

		// Update the id-2-schema cache
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[newSchema.id] = newSchema
		client.idSchemaCacheLock.Unlock()

	}

	return newSchema, nil
}

// SetCredentials allows users to set credentials to be
// used with Schema Registry, for scenarios when Schema
// Registry has authentication enabled.
func (client *SchemaRegistryClient) SetCredentials(username string, password string) {
	if len(username) > 0 && len(password) > 0 {
		credentials := credentials{username, password}
		client.credentials = &credentials
	}
}

// SetTimeout allows the client to be reconfigured about
// how much time internal HTTP requests will take until
// they timeout. FYI, It defaults to five seconds.
func (client *SchemaRegistryClient) SetTimeout(timeout time.Duration) {
	client.httpClient.Timeout = timeout
}

// CachingEnabled allows the client to cache any values
// that have been returned, which may speed up performance
// if these values rarely changes.
func (client *SchemaRegistryClient) CachingEnabled(value bool) {
	client.cachingEnabled = value
}

// CodecCreationEnabled allows the application to enable/disable
// the automatic creation of codec's when schemas are returned.
func (client *SchemaRegistryClient) CodecCreationEnabled(value bool) {
	client.codecCreationEnabled = value
}

func (client *SchemaRegistryClient) getVersion(subject string,
	version string, isKey bool) (*Schema, error) {

	concreteSubject := getConcreteSubject(subject, isKey)

	if client.cachingEnabled {
		cacheKey := cacheKey(concreteSubject, version)
		client.subjectSchemaCacheLock.RLock()
		cachedResult := client.subjectSchemaCache[cacheKey]
		client.subjectSchemaCacheLock.RUnlock()
		if cachedResult != nil {
			return cachedResult, nil
		}
	}

	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectByVersion, concreteSubject, version), nil)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}
	var codec *goavro.Codec
	if client.codecCreationEnabled {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		id:      schemaResp.ID,
		schema:  schemaResp.Schema,
		version: schemaResp.Version,
		codec:   codec,
	}

	if client.cachingEnabled {

		// Update the subject-2-schema cache
		cacheKey := cacheKey(concreteSubject, version)
		client.subjectSchemaCacheLock.Lock()
		client.subjectSchemaCache[cacheKey] = schema
		client.subjectSchemaCacheLock.Unlock()

		// Update the id-2-schema cache
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[schema.id] = schema
		client.idSchemaCacheLock.Unlock()

	}

	return schema, nil
}

func (client *SchemaRegistryClient) httpRequest(method, uri string, payload io.Reader) ([]byte, error) {

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
		return nil, createError(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

// ID ensures access to ID
func (schema *Schema) ID() int {
	return schema.id
}

// Schema ensures access to Schema
func (schema *Schema) Schema() string {
	return schema.schema
}

// Version ensures access to Version
func (schema *Schema) Version() int {
	return schema.version
}

// Codec ensures access to Codec
func (schema *Schema) Codec() *goavro.Codec {
	return schema.codec
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

func createError(resp *http.Response) error {
	decoder := json.NewDecoder(resp.Body)
	var errorResp struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}
	err := decoder.Decode(&errorResp)
	if err == nil {
		return fmt.Errorf("%s: %s", resp.Status, errorResp.Message)
	}
	return fmt.Errorf("%s", resp.Status)
}
