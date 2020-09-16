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

func TestSchemaRegistryClient_CreateSchemaWithReferences(t *testing.T) {
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
				References: []Reference{
					{Name: "test3", Subject: "test4", Version: 1},
				},
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
	schema, err := srClient.CreateSchema("test1", "test2", Protobuf, false, Reference{Name: "test3", Subject: "test4", Version: 1})

	// Test response
	assert.NoError(t, err)
	assert.Equal(t, schema.id, 1)
	assert.Nil(t, schema.codec)
	assert.Equal(t, schema.schema, "test2")
	assert.Equal(t, schema.version, 1)
}
