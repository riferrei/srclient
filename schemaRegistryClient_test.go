package srclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

	{
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
		schema, err := srClient.CreateSchema("test1-value", "test2", Protobuf)

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.id, 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.schema, "test2")
		assert.Equal(t, schema.version, 1)
	}

	{
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
		schema, err := srClient.CreateSchema("test1", "test2", Avro)

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.id, 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.schema, "test2")
		assert.Equal(t, schema.version, 1)
	}
}

func TestSchemaRegistryClient_LookupSchemaWithoutReferences(t *testing.T) {

	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			responsePayload := schemaResponse{
				Subject: "test1",
				Version: 1,
				Schema:  "test2",
				ID:      1,
			}
			response, _ := json.Marshal(responsePayload)
			switch req.URL.String() {
			case "/subjects/test1-value":

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
			default:
				assert.Error(t, errors.New("unhandled request"))
			}

		}))

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		schema, err := srClient.LookupSchema("test1-value", "test2", Protobuf)

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.id, 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.schema, "test2")
		assert.Equal(t, schema.version, 1)
	}

	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			responsePayload := schemaResponse{
				Subject: "test1",
				Version: 1,
				Schema:  "test2",
				ID:      1,
			}
			response, _ := json.Marshal(responsePayload)
			switch req.URL.String() {
			case "/subjects/test1-value":

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
			default:
				assert.Error(t, errors.New("unhandled request"))
			}

		}))

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		schema, err := srClient.LookupSchema("test1-value", "test2", Avro)

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.id, 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.schema, "test2")
		assert.Equal(t, schema.version, 1)
	}

	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			var errorResp struct {
				ErrorCode int    `json:"error_code"`
				Message   string `json:"message"`
			}
			errorResp.ErrorCode = 40401
			errorResp.Message = "Subject 'test1' not found"

			response, _ := json.Marshal(errorResp)
			switch req.URL.String() {
			case "/subjects/test1-value":

				requestPayload := schemaRequest{
					Schema:     "test2",
					SchemaType: Avro.String(),
					References: []Reference{},
				}
				expected, _ := json.Marshal(requestPayload)
				// Test payload
				assert.Equal(t, bodyToString(req.Body), string(expected))
				// Send error response to simulate the subject not being found
				rw.WriteHeader(http.StatusNotFound)
				rw.Write(response)
			default:
				assert.Error(t, errors.New("unhandled request"))
			}

		}))

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		_, err := srClient.LookupSchema("test1-value", "test2", Avro)

		// Test response is 404 error
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "404 Not Found: Subject 'test1' not found")
	}

	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			var errorResp struct {
				ErrorCode int    `json:"error_code"`
				Message   string `json:"message"`
			}
			errorResp.ErrorCode = 40403
			errorResp.Message = "Schema not found"

			response, _ := json.Marshal(errorResp)
			switch req.URL.String() {
			case "/subjects/test1-value":

				requestPayload := schemaRequest{
					Schema:     "test2",
					SchemaType: Avro.String(),
					References: []Reference{},
				}
				expected, _ := json.Marshal(requestPayload)
				// Test payload
				assert.Equal(t, bodyToString(req.Body), string(expected))
				// Send error response to simulate the schema not being found
				rw.WriteHeader(http.StatusNotFound)
				rw.Write(response)
			default:
				assert.Error(t, errors.New("unhandled request"))
			}

		}))

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		_, err := srClient.LookupSchema("test1-value", "test2", Avro)

		// Test response is 404 error
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "404 Not Found: Schema not found")
	}
}

func TestSchemaRegistryClient_GetSchemaByVersionWithReferences(t *testing.T) {
	{
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
		schema, err := srClient.GetSchemaByVersion("test1", 1)

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.ID(), 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.Schema(), "payload")
		assert.Equal(t, schema.Version(), 1)
		assert.Equal(t, schema.References(), refs)
		assert.Equal(t, len(schema.References()), 2)
	}
	{
		server, call := mockServerWithSchemaResponse(t, "test1", "1", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		schema, err := srClient.GetSchemaByVersion("test1", 1)

		// Test response
		assert.NoError(t, err)

		assert.Equal(t, 1, *call)
		assert.Equal(t, schema.ID(), 1)
		assert.Nil(t, schema.codec)
		assert.Equal(t, schema.Schema(), "payload")
		assert.Equal(t, schema.Version(), 1)
		assert.Nil(t, schema.References())
		assert.Equal(t, len(schema.References()), 0)
	}
}

func TestSchemaRegistryClient_GetSchemaByVersionReturnsValueFromCache(t *testing.T) {
	{
		server, call := mockServerWithSchemaResponse(t, "test1", "1", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		schema1, err := srClient.GetSchemaByVersion("test1", 1)

		// Test response
		assert.NoError(t, err)

		// When called twice
		schema2, err := srClient.GetSchemaByVersion("test1", 1)

		assert.NoError(t, err)
		assert.Equal(t, 1, *call)
		assert.Equal(t, schema1, schema2)
	}
}

func TestSchemaRegistryClient_GetLatestSchemaReturnsValueFromCache(t *testing.T) {
	server, call := mockServerWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
		Subject:    "test1",
		Version:    1,
		Schema:     "payload",
		ID:         1,
		References: nil,
	})

	srClient := CreateSchemaRegistryClient(server.URL)
	schema1, err := srClient.GetLatestSchema("test1-value")

	// Test response
	assert.NoError(t, err)

	// When called twice
	schema2, err := srClient.GetLatestSchema("test1-value")

	assert.NoError(t, err)
	assert.Equal(t, 1, *call)
	assert.Equal(t, schema1, schema2)
}

func TestSchemaRegistryClient_JsonSchemaParses(t *testing.T) {
	{
		server, call := mockServerWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "{\"type\": \"object\",\n\"properties\": {\n  \"f1\": {\n    \"type\": \"string\"\n  }}}",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		schema1, err := srClient.GetLatestSchema("test1-value")

		// Test valid schema response
		assert.NoError(t, err)
		assert.Equal(t, 1, *call)
		var v interface{}
		assert.NotNil(t, schema1.JsonSchema())
		assert.NoError(t, json.Unmarshal([]byte("{\"f1\": \"v1\"}"), &v))
		assert.NoError(t, schema1.JsonSchema().Validate(v))
	}
	{
		server, call := mockServerWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		schema1, err := srClient.GetLatestSchema("test1-value")

		// Test invalid schema response
		assert.NoError(t, err)
		assert.Equal(t, 1, *call)
		assert.Nil(t, schema1.JsonSchema())
	}
}

func mockServerWithSchemaResponse(t *testing.T, subject string, version string, schemaResponse schemaResponse) (*httptest.Server, *int) {
	var count int
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		count++
		response, _ := json.Marshal(schemaResponse)

		switch req.URL.String() {
		case fmt.Sprintf("/subjects/%s/versions/%s", subject, version):
			// Send response to be tested
			_, err := rw.Write(response)
			if err != nil {
				t.Errorf("could not write response %s", err)
			}
		default:
			require.Fail(t, "unhandled request")
		}
	})), &count
}
