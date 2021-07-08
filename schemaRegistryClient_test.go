package srclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bodyToString(in io.ReadCloser) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(in)
	return buf.String()
}

func TestSchemaRegistryClient_CreateSchemaWithoutReferences(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		responsePayload := schemaResponse{
			Subject: "test1",
			Version: 1,
			Schema:  "test2",
			ID:      1,
		}
		response, _ := json.Marshal(responsePayload)

		switch req.URL.String() {
		case "/subjects/test1-value/versions":
			requestPayload := schemaRequest{
				Schema:     "test2",
				SchemaType: Protobuf.String(),
				References: []Reference{},
			}
			expected, _ := json.Marshal(requestPayload)
			// Test payload
			assert.Equal(t, bodyToString(req.Body), string(expected))
			// Send response to be tested
			rw.Write(response)
		case "/subjects/test1-value/versions/latest":
			// Send response to be tested
			rw.Write(response)
		default:
			assert.Error(t, errors.New("unhandled request"))
		}

	}))

	srClient := CreateSchemaRegistryClient(server.URL)
	srClient.CodecCreationEnabled(false)
	schema, err := srClient.CreateSchema("test1", "test2", Protobuf, false)

	// Test response
	assert.NoError(t, err)
	assert.Equal(t, schema.id, 1)
	assert.Nil(t, schema.codec)
	assert.Equal(t, schema.schema, "test2")
	assert.Equal(t, schema.version, 1)
}

func TestSchemaRegistryClient_CreateSchemaWithArbitrarySubjectName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		responsePayload := schemaResponse{
			Subject: "test1",
			Version: 1,
			Schema:  "test2",
			ID:      1,
		}
		response, _ := json.Marshal(responsePayload)

		switch req.URL.String() {
		case "/subjects/test1/versions":
			requestPayload := schemaRequest{
				Schema:     "test2",
				SchemaType: Avro.String(),
				References: []Reference{},
			}
			expected, _ := json.Marshal(requestPayload)
			// Test payload
			assert.Equal(t, bodyToString(req.Body), string(expected))
			// Send response to be tested
			rw.Write(response)
		case "/subjects/test1/versions/latest":
			// Send response to be tested
			rw.Write(response)
		default:
			assert.Error(t, errors.New("unhandled request"))
		}

	}))

	srClient := CreateSchemaRegistryClient(server.URL)
	srClient.CodecCreationEnabled(false)
	schema, err := srClient.CreateSchemaWithArbitrarySubject("test1", "test2", Avro)

	// Test response
	assert.NoError(t, err)
	assert.Equal(t, schema.id, 1)
	assert.Nil(t, schema.codec)
	assert.Equal(t, schema.schema, "test2")
	assert.Equal(t, schema.version, 1)
}

func TestSchemaRegistryClient_GetSchemaByVersionWithArbitrarySubjectWithReferences(t *testing.T) {
	refs := []Reference{
		{Name: "name1", Subject: "subject1", Version: 1},
		{Name: "name2", Subject: "subject2", Version: 2},
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		responsePayload := schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: refs,
		}
		response, _ := json.Marshal(responsePayload)

		switch req.URL.String() {
		case "/subjects/test1/versions/1":
			// Send response to be tested
			rw.Write(response)
		default:
			require.Fail(t, "unhandled request")
		}

	}))

	srClient := CreateSchemaRegistryClient(server.URL)
	srClient.CodecCreationEnabled(false)
	schema, err := srClient.GetSchemaByVersionWithArbitrarySubject("test1", 1)

	// Test response
	assert.NoError(t, err)
	assert.Equal(t, schema.ID(), 1)
	assert.Nil(t, schema.codec)
	assert.Equal(t, schema.Schema(), "payload")
	assert.Equal(t, schema.Version(), 1)
	assert.Equal(t, schema.References(), refs)
	assert.Equal(t, len(schema.References()), 2)
}

func TestSchemaRegistryClient_GetSchemaByVersionWithArbitrarySubjectWithoutReferences(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		responsePayload := schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		}
		response, _ := json.Marshal(responsePayload)

		switch req.URL.String() {
		case "/subjects/test1/versions/1":
			// Send response to be tested
			rw.Write(response)
		default:
			require.Fail(t, "unhandled request")
		}

	}))

	srClient := CreateSchemaRegistryClient(server.URL)
	srClient.CodecCreationEnabled(false)
	schema, err := srClient.GetSchemaByVersionWithArbitrarySubject("test1", 1)

	// Test response
	assert.NoError(t, err)
	assert.Equal(t, schema.ID(), 1)
	assert.Nil(t, schema.codec)
	assert.Equal(t, schema.Schema(), "payload")
	assert.Equal(t, schema.Version(), 1)
	assert.Nil(t, schema.References())
	assert.Equal(t, len(schema.References()), 0)
}
