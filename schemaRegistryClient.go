package srclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/linkedin/goavro"
)

// SchemaRegistryClient allows interactions with
// Schema Registry over HTTP. Applications using
// this client can retrieve data about schemas,
// which in turn can be used to serialize and
// deserialize records.
type SchemaRegistryClient struct {
	schemaRegistryURL      string
	credentials            *credentials
	httpClient             *http.Client
	cachingEnabled         bool
	idSchemaCache          map[int]*Schema
	idSchemaCacheLock      sync.RWMutex
	subjectSchemaCache     map[string]*Schema
	subjectSchemaCacheLock sync.RWMutex
}

// Schema is a data structure that holds the
// ID from a schema (the ID that Schema Registry
// uses) and a codec created from the textual
// representation of the schema associated with
// the ID.
type Schema struct {
	ID    int
	Codec *goavro.Codec
}

type credentials struct {
	username string
	password string
}

type schemaRequest struct {
	Schema string `json:"schema"`
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
	contentType      = "application/vnd.schemaregistry.v1+json"
)

// CreateSchemaRegistryClient creates a client that allows
// applications to interact with Schema Registry over HTTP.
func CreateSchemaRegistryClient(schemaRegistryURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{schemaRegistryURL: schemaRegistryURL,
		httpClient:         &http.Client{Timeout: 5 * time.Second},
		idSchemaCache:      make(map[int]*Schema),
		subjectSchemaCache: make(map[string]*Schema)}
}

// GetSchema retrieves a concrete schema that applications
// can use to serialize and deserialize records. The schema
// contains the ID that uniquely identifies that schema, as
// well as a codec created from the textual representation
// of the schema associated with the ID.
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

	codec, err := goavro.NewCodec(schemaResp.Schema)
	schema := &Schema{ID: schemaID, Codec: codec}
	if client.cachingEnabled && err == nil {
		client.idSchemaCacheLock.Lock()
		client.idSchemaCache[schemaID] = schema
		client.idSchemaCacheLock.Unlock()
	}
	return schema, nil

}

// GetLatestSchema retrieves a concrete schema that applications
// can use to serialize and deserialize records. The schema is
// in fact the latest version of the given subject. The schema
// contains the ID that uniquely identifies that schema, as
// well as a codec created from the textual representation
// of the schema associated with the ID.
func (client *SchemaRegistryClient) GetLatestSchema(subject string, isKey bool) (*Schema, error) {
	return client.getSchemaByVersion(subject, "latest", isKey)
}

// GetSchemaVersions returns a list of versions of a given
// subject. This allows applications to eventually return
// the concrete schema for a given version.
func (client *SchemaRegistryClient) GetSchemaVersions(subject string, isKey bool) ([]int, error) {

	subject = getConcreteSubject(subject, isKey)
	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectVersions, subject), nil)
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

// GetSchemaByVersion retrieves a concrete schema that applications
// can use to serialize and deserialize records. The schema is
// based on the version provided for the given subject. The schema
// contains the ID that uniquely identifies that schema, as
// well as a codec created from the textual representation
// of the schema associated with the ID.
func (client *SchemaRegistryClient) GetSchemaByVersion(subject string, version int, isKey bool) (*Schema, error) {
	return client.getSchemaByVersion(subject, fmt.Sprintf("%d", version), isKey)
}

// CreateSubject creates a new schema in Schema Registry and
// associates this schema with the subject provided. It returns
// a the new schema that contains the ID that uniquely identifies
// that schema, as well as a codec created from the textual
// representation of the schema associated with the ID.
func (client *SchemaRegistryClient) CreateSubject(subject string, schema string, isKey bool) (*Schema, error) {

	if client.cachingEnabled {
		client.subjectSchemaCacheLock.RLock()
		cachedSchema := client.subjectSchemaCache[subject]
		client.subjectSchemaCacheLock.RUnlock()
		if cachedSchema != nil {
			return cachedSchema, nil
		}
	}

	schemaReq := schemaRequest{Schema: schema}
	schemaBytes, err := json.Marshal(schemaReq)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewBuffer(schemaBytes)
	subject = getConcreteSubject(subject, isKey)
	resp, err := client.httpRequest("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	newSchema := &Schema{ID: schemaResp.ID, Codec: codec}
	if client.cachingEnabled && err == nil {
		client.subjectSchemaCacheLock.Lock()
		client.subjectSchemaCache[subject] = newSchema
		client.subjectSchemaCacheLock.Unlock()
	}
	return newSchema, nil

}

// SetCredentials allows users to set credentials to be
// used with Schema Registry, for scenarios when Schema
// Registry had authentication enabled in the service.
func (client *SchemaRegistryClient) SetCredentials(username string, password string) {
	if len(username) > 0 && len(password) > 0 {
		credentials := credentials{username, password}
		client.credentials = &credentials
	}
}

// SetTimeout allows the client to be reconfigured about
// how much time internal HTTP requests will take until
// they timeout. It defaults to five seconds, FYI.
func (client *SchemaRegistryClient) SetTimeout(timeout time.Duration) {
	client.httpClient.Timeout = timeout
}

// EnableCaching allows application to cache any values
// that have been returned via this client, which may
// speed up the performance if these values never changes.
func (client *SchemaRegistryClient) EnableCaching(value bool) {
	client.cachingEnabled = value
}

func (client *SchemaRegistryClient) getSchemaByVersion(subject string,
	version string, isKey bool) (*Schema, error) {

	if client.cachingEnabled {
		client.subjectSchemaCacheLock.RLock()
		cachedResult := client.subjectSchemaCache[subject]
		client.subjectSchemaCacheLock.RUnlock()
		if cachedResult != nil {
			return cachedResult, nil
		}
	}

	subject = getConcreteSubject(subject, isKey)
	resp, err := client.httpRequest("GET", fmt.Sprintf(subjectByVersion, subject, version), nil)
	if err != nil {
		return nil, err
	}

	schemaResp := new(schemaResponse)
	err = json.Unmarshal(resp, &schemaResp)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schemaResp.Schema)
	if err != nil {
		return nil, err
	}
	var schema = &Schema{
		ID:    schemaResp.ID,
		Codec: codec,
	}
	if client.cachingEnabled && err == nil {
		client.subjectSchemaCacheLock.Lock()
		client.subjectSchemaCache[subject] = schema
		client.subjectSchemaCacheLock.Unlock()
	}
	return schema, nil

}

func getConcreteSubject(subject string, isKey bool) string {
	if isKey {
		subject = fmt.Sprintf("%s-key", subject)
	} else {
		subject = fmt.Sprintf("%s-value", subject)
	}
	return subject
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
