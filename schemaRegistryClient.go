package srclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"golang.org/x/sync/semaphore"
)

const defaultSemaphoreWeight int64 = 16
const defaultTimeout = 5 * time.Second

// ISchemaRegistryClient provides the
// definition of the operations that
// this Schema Registry client provides.
type ISchemaRegistryClient interface {
	GetGlobalCompatibilityLevel() (*CompatibilityLevel, error)
	GetCompatibilityLevel(subject string, defaultToGlobal bool) (*CompatibilityLevel, error)
	GetSubjects() ([]string, error)
	GetSubjectsIncludingDeleted() ([]string, error)
	GetSchema(schemaID int) (*Schema, error)
	GetLatestSchema(subject string) (*Schema, error)
	GetSchemaVersions(subject string) ([]int, error)
	GetSubjectVersionsById(schemaID int) (SubjectVersionResponse, error)
	GetSchemaByVersion(subject string, version int) (*Schema, error)
	GetSchemaRegistryURL() string
	CreateSchema(subject string, schema string, schemaType SchemaType, references ...Reference) (*Schema, error)
	LookupSchema(subject string, schema string, schemaType SchemaType, references ...Reference) (*Schema, error)
	ChangeSubjectCompatibilityLevel(subject string, compatibility CompatibilityLevel) (*CompatibilityLevel, error)
	DeleteSubjectCompatibilityLevel(subject string) (*CompatibilityLevel, error)
	DeleteSubject(subject string, permanent bool) error
	DeleteSubjectByVersion(subject string, version int, permanent bool) error
	SetCredentials(username string, password string)
	SetBearerToken(token string)
	SetTimeout(timeout time.Duration)
	CachingEnabled(value bool)
	ResetCache()
	CodecCreationEnabled(value bool)
	CodecJsonEnabled(value bool)
	IsSchemaCompatible(subject, schema, version string, schemaType SchemaType, references ...Reference) (bool, error)
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
	codecAsFullJson          bool
	codecCreationEnabledLock sync.RWMutex
	idSchemaCache            map[int]*Schema
	idSchemaCacheLock        sync.RWMutex
	subjectSchemaCache       map[string]*Schema
	subjectSchemaCacheLock   sync.RWMutex
	sem                      *semaphore.Weighted
	preReqFn                 func(req *http.Request) error
}

var _ ISchemaRegistryClient = new(SchemaRegistryClient)

type SchemaType string

const (
	Protobuf SchemaType = "PROTOBUF"
	Avro     SchemaType = "AVRO"
	Json     SchemaType = "JSON"
)

func (s SchemaType) String() string {
	// Avro is the default schemaType.
	// Returning "" omits schemaType from the schemaRequest JSON for compatibility with older API versions.
	if s == Avro {
		return ""
	}

	return string(s)
}

type CompatibilityLevel string

const (
	None               CompatibilityLevel = "NONE"
	Backward           CompatibilityLevel = "BACKWARD"
	BackwardTransitive CompatibilityLevel = "BACKWARD_TRANSITIVE"
	Forward            CompatibilityLevel = "FORWARD"
	ForwardTransitive  CompatibilityLevel = "FORWARD_TRANSITIVE"
	Full               CompatibilityLevel = "FULL"
	FullTransitive     CompatibilityLevel = "FULL_TRANSITIVE"
)

func (s CompatibilityLevel) String() string {
	return string(s)
}

// Reference references use the import statement of Protobuf and
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
	id         int
	schema     string
	schemaType *SchemaType
	version    int
	references []Reference
	codec      *goavro.Codec
	jsonSchema *jsonschema.Schema
}

// credentials can have either username AND password
// OR a bearerToken, it cannot have both forms of authentication
type credentials struct {
	// Username and Password for Schema Basic Auth
	username string
	password string

	// Bearer Authorization token
	bearerToken string
}

type schemaRequest struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
}

type schemaResponse struct {
	Subject    string      `json:"subject"`
	Version    int         `json:"version"`
	Schema     string      `json:"schema"`
	SchemaType *SchemaType `json:"schemaType"`
	ID         int         `json:"id"`
	References []Reference `json:"references"`
}

type isCompatibleResponse struct {
	IsCompatible bool `json:"is_compatible"`
}

type configResponse struct {
	CompatibilityLevel CompatibilityLevel `json:"compatibilityLevel"`
}

type configChangeRequest struct {
	CompatibilityLevel CompatibilityLevel `json:"compatibility"`
}

type configChangeResponse configChangeRequest

type SubjectVersionResponse []subjectVersionPair

type subjectVersionPair struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

const (
	schemaByID          = "/schemas/ids/%d"
	subjectVersionsByID = "/schemas/ids/%d/versions"
	subjectBySubject    = "/subjects/%s"
	subjectVersions     = "/subjects/%s/versions"
	subjectByVersion    = "/subjects/%s/versions/%s"
	subjects            = "/subjects"
	config              = "/config"
	configBySubject     = "/config/%s"
	contentType         = "application/vnd.schemaregistry.v1+json"
)

// Option serves as an input for NewSchemaRegistryClient
type Option func(*SchemaRegistryClient)

// WithClient is used in NewSchemaRegistryClient to override the default client
func WithClient(client *http.Client) Option {
	return func(registryConfig *SchemaRegistryClient) {
		registryConfig.httpClient = client
	}
}

// WithSemaphoreWeight is used in NewSchemaRegistryClient to override the default semaphoreWeight
func WithSemaphoreWeight(semaphoreWeight int64) Option {
	return func(client *SchemaRegistryClient) {
		client.sem = semaphore.NewWeighted(semaphoreWeight)
	}
}

func WithPreReqFn(preReq func(req *http.Request) error) Option {
	return func(registryConfig *SchemaRegistryClient) {
		registryConfig.preReqFn = preReq
	}
}

// NewSchemaRegistryClient creates a client that allows
// interactions with Schema Registry over HTTP. Applications
// using this client can retrieve data about schemas, which
// in turn can be used to serialize and deserialize records.
func NewSchemaRegistryClient(schemaRegistryURL string, options ...Option) *SchemaRegistryClient {
	client := &SchemaRegistryClient{
		schemaRegistryURL:    schemaRegistryURL,
		httpClient:           &http.Client{Timeout: defaultTimeout},
		cachingEnabled:       true,
		codecCreationEnabled: false,
		idSchemaCache:        make(map[int]*Schema),
		subjectSchemaCache:   make(map[string]*Schema),
		sem:                  semaphore.NewWeighted(defaultSemaphoreWeight),
	}
	for _, option := range options {
		option(client)
	}

	return client
}

// CreateSchemaRegistryClient creates a client that allows
// interactions with Schema Registry over HTTP. Applications
// using this client can retrieve data about schemas, which
// in turn can be used to serialize and deserialize records.
// Deprecated: Prefer NewSchemaRegistryClient(schemaRegistryURL)
func CreateSchemaRegistryClient(schemaRegistryURL string) *SchemaRegistryClient {
	return NewSchemaRegistryClient(schemaRegistryURL)
}

// CreateSchemaRegistryClientWithOptions provides the ability to pass the http.Client to be used, as well as the semaphoreWeight for concurrent requests
// Deprecated: Prefer NewSchemaRegistryClient(schemaRegistryURL, WithClient(*http.Client), WithSemaphoreWeight(int64))
func CreateSchemaRegistryClientWithOptions(schemaRegistryURL string, client *http.Client, semaphoreWeight int) *SchemaRegistryClient {
	return NewSchemaRegistryClient(schemaRegistryURL, WithClient(client), WithSemaphoreWeight(int64(semaphoreWeight)))
}

// GetSchemaRegistryURL returns the URL of the Schema Registry
func (client *SchemaRegistryClient) GetSchemaRegistryURL() string {
	return client.schemaRegistryURL
}

// ResetCache resets the schema caches to be able to get updated schemas.
func (client *SchemaRegistryClient) ResetCache() {
	client.idSchemaCacheLock.Lock()
	client.subjectSchemaCacheLock.Lock()
	client.idSchemaCache = make(map[int]*Schema)
	client.subjectSchemaCache = make(map[string]*Schema)
	client.idSchemaCacheLock.Unlock()
	client.subjectSchemaCacheLock.Unlock()
}

// GetSchema gets the schema associated with the given id.
func (client *SchemaRegistryClient) GetSchema(schemaID int) (*Schema, error) {

	if client.getCachingEnabled() {
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
	if err := json.Unmarshal(resp, &schemaResp); err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if client.getCodecCreationEnabled() {
		codec, err = client.getCodecForSchema(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}

	var schema = &Schema{
		id:         schemaID,
		schema:     schemaResp.Schema,
		version:    schemaResp.Version,
		schemaType: schemaResp.SchemaType,
		references: schemaResp.References,
		codec:      codec,
	}

	if client.getCachingEnabled() {
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[schemaID] = schema
		client.idSchemaCacheLock.Unlock()
	}

	return schema, nil
}

// GetLatestSchema gets the schema associated with the given subject.
// The schema returned contains the last version for that subject.
func (client *SchemaRegistryClient) GetLatestSchema(subject string) (*Schema, error) {
	return client.getVersion(subject, "latest")
}

// GetSubjectVersionsById returns subject-version pairs identified by the schema ID.
func (client *SchemaRegistryClient) GetSubjectVersionsById(schemaID int) (SubjectVersionResponse, error) {
	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectVersionsByID, schemaID), nil)
	if err != nil {
		return nil, err
	}

	var response = new(SubjectVersionResponse)
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return nil, err
	}

	return *response, nil
}

// GetSchemaVersions returns a list of versions from a given subject.
func (client *SchemaRegistryClient) GetSchemaVersions(subject string) ([]int, error) {
	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectVersions, url.PathEscape(subject)), nil)
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

// ChangeSubjectCompatibilityLevel changes the compatibility level of the subject.
func (client *SchemaRegistryClient) ChangeSubjectCompatibilityLevel(subject string, compatibility CompatibilityLevel) (*CompatibilityLevel, error) {
	configChangeReq := configChangeRequest{CompatibilityLevel: compatibility}
	configChangeReqBytes, err := json.Marshal(configChangeReq)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewBuffer(configChangeReqBytes)

	resp, err := client.httpRequest("PUT", fmt.Sprintf(configBySubject, url.PathEscape(subject)), payload)
	if err != nil {
		return nil, err
	}

	var cfgChangeResp = new(configChangeResponse)
	err = json.Unmarshal(resp, &cfgChangeResp)
	if err != nil {
		return nil, err
	}

	return &cfgChangeResp.CompatibilityLevel, nil
}

// DeleteSubjectCompatibilityLevel deletes subject-level compatibility level config and reverts to the global default.
func (client *SchemaRegistryClient) DeleteSubjectCompatibilityLevel(subject string) (*CompatibilityLevel, error) {
	resp, err := client.httpRequest("DELETE", fmt.Sprintf(configBySubject, url.QueryEscape(subject)), nil)
	if err != nil {
		return nil, err
	}
	var cfgChangeResp = new(configChangeResponse)
	err = json.Unmarshal(resp, &cfgChangeResp)
	if err != nil {
		return nil, err
	}
	return &cfgChangeResp.CompatibilityLevel, nil
}

// GetGlobalCompatibilityLevel returns the global compatibility level of the registry.
func (client *SchemaRegistryClient) GetGlobalCompatibilityLevel() (*CompatibilityLevel, error) {
	resp, err := client.httpRequest("GET", config, nil)
	if err != nil {
		return nil, err
	}

	var configResponse = new(configResponse)
	err = json.Unmarshal(resp, &configResponse)
	if err != nil {
		return nil, err
	}

	return &configResponse.CompatibilityLevel, nil
}

// GetCompatibilityLevel returns the compatibility level of the subject.
// If defaultToGlobal is set to true and no compatibility level is set on the subject, the global compatibility level is returned.
func (client *SchemaRegistryClient) GetCompatibilityLevel(subject string, defaultToGlobal bool) (*CompatibilityLevel, error) {
	resp, err := client.httpRequest("GET", fmt.Sprintf(configBySubject+"?defaultToGlobal=%t", url.PathEscape(subject), defaultToGlobal), nil)
	if err != nil {
		return nil, err
	}

	var configResponse = new(configResponse)
	if err := json.Unmarshal(resp, &configResponse); err != nil {
		return nil, err
	}

	return &configResponse.CompatibilityLevel, nil
}

// GetSubjects returns a list of all subjects in the registry
func (client *SchemaRegistryClient) GetSubjects() ([]string, error) {
	resp, err := client.httpRequest("GET", subjects, nil)
	if err != nil {
		return nil, err
	}

	var allSubjects []string
	if err = json.Unmarshal(resp, &allSubjects); err != nil {
		return nil, err
	}

	return allSubjects, nil
}

// GetSubjectsIncludingDeleted returns a list of all subjects in the registry including those which have been soft deleted
func (client *SchemaRegistryClient) GetSubjectsIncludingDeleted() ([]string, error) {
	resp, err := client.httpRequest("GET", subjects+"?deleted=true", nil)
	if err != nil {
		return nil, err
	}
	var allSubjects []string
	if err = json.Unmarshal(resp, &allSubjects); err != nil {
		return nil, err
	}

	return allSubjects, nil
}

// GetSchemaByVersion gets the schema associated with the given subject.
// The schema returned contains the version specified as a parameter.
func (client *SchemaRegistryClient) GetSchemaByVersion(subject string, version int) (*Schema, error) {
	return client.getVersion(subject, strconv.Itoa(version))
}

// CreateSchema creates a new schema in Schema Registry and associates
// with the subject provided. It returns the newly created schema with
// all its associated information.
func (client *SchemaRegistryClient) CreateSchema(subject string, schema string,
	schemaType SchemaType, references ...Reference) (*Schema, error) {
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
	resp, err := client.httpRequest("POST", fmt.Sprintf(subjectVersions, url.PathEscape(subject)), payload)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}

	newSchema, err := client.GetSchema(schemaResp.ID)
	if err != nil {
		return nil, err
	}

	if client.getCachingEnabled() {

		// Update the subject-2-schema cache
		cacheKey := cacheKey(subject,
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

// LookupSchema looks up the schema by subject and schema string. If it finds the schema it returns it with all its associated information.
func (client *SchemaRegistryClient) LookupSchema(subject string, schema string, schemaType SchemaType, references ...Reference) (*Schema, error) {
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
	resp, err := client.httpRequest("POST", fmt.Sprintf(subjectBySubject, url.PathEscape(subject)), payload)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if client.getCodecCreationEnabled() && schemaType == Avro {
		codec, err = client.getCodecForSchema(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var gotSchema = &Schema{
		id:         schemaResp.ID,
		schema:     schemaResp.Schema,
		schemaType: schemaResp.SchemaType,
		version:    schemaResp.Version,
		references: schemaResp.References,
		codec:      codec,
	}

	if client.getCachingEnabled() {

		// Update the subject-2-schema cache
		cacheKey := cacheKey(subject,
			strconv.Itoa(gotSchema.version))
		client.subjectSchemaCacheLock.Lock()
		client.subjectSchemaCache[cacheKey] = gotSchema
		client.subjectSchemaCacheLock.Unlock()

		// Update the id-2-schema cache
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[gotSchema.id] = gotSchema
		client.idSchemaCacheLock.Unlock()

	}

	return gotSchema, nil
}

// IsSchemaCompatible checks if the given schema is compatible with the given subject and version
// valid versions are versionID and "latest"
func (client *SchemaRegistryClient) IsSchemaCompatible(subject, schema, version string, schemaType SchemaType, references ...Reference) (bool, error) {
	if references == nil {
		references = make([]Reference, 0)
	}

	schemaReq := schemaRequest{Schema: schema, SchemaType: schemaType.String(), References: references}
	schemaReqBytes, err := json.Marshal(schemaReq)
	if err != nil {
		return false, err
	}
	payload := bytes.NewBuffer(schemaReqBytes)

	url := fmt.Sprintf("/compatibility/subjects/%s/versions/%s", url.PathEscape(subject), version)
	resp, err := client.httpRequest("POST", url, payload)
	if err != nil {
		return false, err
	}

	compatibilityResponse := new(isCompatibleResponse)
	err = json.Unmarshal(resp, compatibilityResponse)
	if err != nil {
		return false, err
	}

	return compatibilityResponse.IsCompatible, nil
}

// DeleteSubject deletes
func (client *SchemaRegistryClient) DeleteSubject(subject string, permanent bool) error {
	uri := "/subjects/" + url.PathEscape(subject)

	if permanent {
		_, err := client.httpRequest("DELETE", uri+"?permanent=true", nil)
		return err
	}

	_, err := client.httpRequest("DELETE", uri, nil)
	return err
}

// DeleteSubjectByVersion deletes the version of the scheme
func (client *SchemaRegistryClient) DeleteSubjectByVersion(subject string, version int, permanent bool) error {
	uri := fmt.Sprintf(subjectByVersion, url.PathEscape(subject), strconv.Itoa(version))
	_, err := client.httpRequest("DELETE", uri, nil)
	if err != nil || !permanent {
		return err
	}

	uri += "?permanent=true"
	_, err = client.httpRequest("DELETE", uri, nil)
	return err
}

// SetCredentials allows users to set credentials to be
// used with Schema Registry, for scenarios when Schema
// Registry has authentication enabled.
func (client *SchemaRegistryClient) SetCredentials(username string, password string) {
	if len(username) > 0 && len(password) > 0 {
		credentials := credentials{username: username, password: password, bearerToken: ""}
		client.credentials = &credentials
	}
}

// SetBearerToken allows users to add a Bearer Token
// http header with calls to Schema Registry
// The BearerToken will override Schema Registry credentials
func (client *SchemaRegistryClient) SetBearerToken(token string) {
	if len(token) > 0 {
		credentials := credentials{username: "", password: "", bearerToken: token}
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
	client.cachingEnabledLock.Lock()
	defer client.cachingEnabledLock.Unlock()
	client.cachingEnabled = value
}

// CodecCreationEnabled allows the application to enable/disable
// the automatic creation of codec's when schemas are returned.
func (client *SchemaRegistryClient) CodecCreationEnabled(value bool) {
	client.codecCreationEnabledLock.Lock()
	defer client.codecCreationEnabledLock.Unlock()
	client.codecCreationEnabled = value
}

// CodecJsonEnabled allows the application to create codec,
// which will serialize/deserialize data as standard json.
// Should be used with CodecCreationEnabled, otherwise it will be ignored.
func (client *SchemaRegistryClient) CodecJsonEnabled(value bool) {
	client.codecCreationEnabledLock.Lock()
	defer client.codecCreationEnabledLock.Unlock()
	client.codecAsFullJson = value
}

func (client *SchemaRegistryClient) getVersion(subject string, version string) (*Schema, error) {

	if client.getCachingEnabled() {
		cacheKey := cacheKey(subject, version)
		client.subjectSchemaCacheLock.RLock()
		cachedResult := client.subjectSchemaCache[cacheKey]
		client.subjectSchemaCacheLock.RUnlock()
		if cachedResult != nil {
			return cachedResult, nil
		}
	}

	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectByVersion, url.PathEscape(subject), version), nil)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}
	var codec *goavro.Codec
	if client.getCodecCreationEnabled() {
		codec, err = client.getCodecForSchema(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		id:         schemaResp.ID,
		schema:     schemaResp.Schema,
		schemaType: schemaResp.SchemaType,
		version:    schemaResp.Version,
		references: schemaResp.References,
		codec:      codec,
	}

	if client.getCachingEnabled() {

		// Update the subject-2-schema cache
		cacheKey := cacheKey(subject, version)
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
		if len(client.credentials.username) > 0 && len(client.credentials.password) > 0 {
			req.SetBasicAuth(client.credentials.username, client.credentials.password)
		} else if len(client.credentials.bearerToken) > 0 {
			if strings.Contains(strings.ToLower(uri), "confluent.cloud") {
				req.Header.Add("Authorization", "Basic "+client.credentials.bearerToken)
			} else {
				req.Header.Add("Authorization", "Bearer "+client.credentials.bearerToken)
			}
		}
	}
	req.Header.Set("Content-Type", contentType)

	if client.preReqFn != nil {
		if err := client.preReqFn(req); err != nil {
			return nil, fmt.Errorf("pre-request function failed: %w", err)
		}
	}

	client.sem.Acquire(context.Background(), 1)
	defer client.sem.Release(1)
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

func (client *SchemaRegistryClient) getCachingEnabled() bool {
	client.cachingEnabledLock.RLock()
	defer client.cachingEnabledLock.RUnlock()
	return client.cachingEnabled
}

func (client *SchemaRegistryClient) getCodecCreationEnabled() bool {
	client.codecCreationEnabledLock.RLock()
	defer client.codecCreationEnabledLock.RUnlock()
	return client.codecCreationEnabled
}

func (client *SchemaRegistryClient) getCodecForSchema(schema string) (*goavro.Codec, error) {
	client.codecCreationEnabledLock.RLock()
	defer client.codecCreationEnabledLock.RUnlock()
	if client.codecAsFullJson {
		return goavro.NewCodecForStandardJSONFull(schema)
	}
	return goavro.NewCodec(schema)
}

// NewSchema instantiates a new Schema struct.
func NewSchema(
	id int,
	schema string,
	schemaType SchemaType,
	version int,
	references []Reference,
	codec *goavro.Codec,
	jsonSchema *jsonschema.Schema,
) (*Schema, error) {
	if schema == "" {
		return nil, errors.New("schema cannot be nil")
	}
	return &Schema{
		id:         id,
		schema:     schema,
		schemaType: &schemaType,
		version:    version,
		references: references,
		codec:      codec,
		jsonSchema: jsonSchema,
	}, nil
}

// ID ensures access to ID
func (schema *Schema) ID() int {
	return schema.id
}

// Schema ensures access to Schema
func (schema *Schema) Schema() string {
	return schema.schema
}

// SchemaType ensures access to SchemaType
func (schema *Schema) SchemaType() *SchemaType {
	return schema.schemaType
}

// Version ensures access to Version
func (schema *Schema) Version() int {
	return schema.version
}

// References ensures access to References
func (schema *Schema) References() []Reference {
	return schema.references
}

// Codec ensures access to Codec
// Will try to initialize a new one if it hasn't been initialized before
// Will return nil if it can't initialize a codec from the schema
func (schema *Schema) Codec() *goavro.Codec {
	if schema.codec == nil {
		codec, err := goavro.NewCodec(schema.Schema())
		if err == nil {
			schema.codec = codec
		}
	}
	return schema.codec
}

// JsonSchema ensures access to JsonSchema
// Will try to initialize a new one if it hasn't been initialized before
// Will return nil if it can't initialize a json schema from the schema
func (schema *Schema) JsonSchema() *jsonschema.Schema {
	if schema.jsonSchema == nil {
		jsonSchema, err := jsonschema.CompileString("schema.json", schema.Schema())
		if err == nil {
			schema.jsonSchema = jsonSchema
		}
	}
	return schema.jsonSchema
}

func cacheKey(subject string, version string) string {
	return fmt.Sprintf("%s-%s", subject, version)
}

// Error implements error, encodes HTTP errors from Schema Registry.
type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
	str     *bytes.Buffer
}

func (e Error) Error() string {
	return e.str.String()
}

func createError(resp *http.Response) error {
	err := Error{str: bytes.NewBuffer(make([]byte, 0))}
	decoder := json.NewDecoder(io.TeeReader(resp.Body, err.str))
	marshalErr := decoder.Decode(&err)
	if marshalErr != nil {
		return fmt.Errorf("%s", resp.Status)
	}

	return err
}
