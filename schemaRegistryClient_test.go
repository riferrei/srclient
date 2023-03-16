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

	"github.com/linkedin/goavro/v2"
	"github.com/santhosh-tekuri/jsonschema/v5"
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
			case fmt.Sprintf("/schemas/ids/%d", responsePayload.Version):
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
			case fmt.Sprintf("/schemas/ids/%d", responsePayload.Version):
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
	var errorCode int
	var errorMessage string
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
		errorCode = 40401
		errorMessage = "Subject 'test1' not found"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			var errorResp struct {
				ErrorCode int    `json:"error_code"`
				Message   string `json:"message"`
			}
			errorResp.ErrorCode = errorCode
			errorResp.Message = errorMessage

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
		castedErr, ok := err.(Error)
		assert.True(t, ok, "convert api error to Error struct")
		assert.Equal(t, errorCode, castedErr.Code)
		assert.Equal(t, errorMessage, castedErr.Message)
	}

	{
		errorCode = 40403
		errorMessage = "Schema not found"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			var errorResp struct {
				ErrorCode int    `json:"error_code"`
				Message   string `json:"message"`
			}
			errorResp.ErrorCode = errorCode
			errorResp.Message = errorMessage

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
		castedErr, ok := err.(Error)
		assert.True(t, ok, "convert api error to Error struct")
		assert.Equal(t, errorCode, castedErr.Code)
		assert.Equal(t, errorMessage, castedErr.Message)
	}
}

func TestSchemaRegistryClient_GetSchemaByIDWithReferences(t *testing.T) {
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
			case "/schemas/ids/1":
				// Send response to be tested
				rw.Write(response)
			default:
				require.Fail(t, "unhandled request")
			}

		}))

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		schema, err := srClient.GetSchema(1)

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
		server, call := mockServerFromIDWithSchemaResponse(t, 1, schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		srClient.CodecCreationEnabled(false)
		schema, err := srClient.GetSchema(1)

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
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1", "1", schemaResponse{
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
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1", "1", schemaResponse{
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
	server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
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

func TestSchemaRegistryClient_GetSchemaType(t *testing.T) {
	{
		expectedSchemaType := Json
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			SchemaType: &expectedSchemaType,
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		schema, err := srClient.GetLatestSchema("test1-value")

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, 1, *call)
		assert.Equal(t, *schema.SchemaType(), expectedSchemaType)
	}
	{
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
			Subject:    "test1",
			Version:    1,
			Schema:     "payload",
			ID:         1,
			References: nil,
		})

		srClient := CreateSchemaRegistryClient(server.URL)
		schema, err := srClient.GetLatestSchema("test1-value")

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, 1, *call)
		assert.Nil(t, schema.SchemaType())
	}
}

func TestSchemaRegistryClient_JsonSchemaParses(t *testing.T) {
	{
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
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
		server, call := mockServerFromSubjectVersionPairWithSchemaResponse(t, "test1-value", "latest", schemaResponse{
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

func TestNewSchema(t *testing.T) {
	const (
		anId        = 3
		aSchema     = "payload"
		aSchemaType = Protobuf
		aVersion    = 2
	)
	var (
		reference1 Reference = Reference{
			Name:    "reference1",
			Subject: "subject1",
			Version: 5,
		}
		reference2 Reference = Reference{
			Name:    "reference2",
			Subject: "subject2",
			Version: 2,
		}
		references                    = []Reference{reference1, reference2}
		jsonSchema *jsonschema.Schema = &jsonschema.Schema{
			Location: "aLocation",
		}
	)
	mockCodec, _ := goavro.NewCodec(`"string"`)
	{
		_, err := NewSchema(anId, "", aSchemaType, aVersion, nil, nil, nil)
		assert.EqualError(t, err, "schema cannot be nil")
	}
	{
		schema, err := NewSchema(anId, aSchema, aSchemaType, aVersion, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, anId, schema.ID())
		assert.Equal(t, aSchema, schema.Schema())
		assert.Equal(t, aSchemaType, *schema.SchemaType())
		assert.Equal(t, aVersion, schema.Version())
		assert.Nil(t, schema.References())
		assert.Nil(t, schema.Codec())
		assert.Nil(t, schema.JsonSchema())

	}
	{
		schema, err := NewSchema(
			anId,
			aSchema,
			aSchemaType,
			aVersion,
			references,
			mockCodec,
			jsonSchema,
		)
		assert.NoError(t, err)
		assert.Equal(t, anId, schema.ID())
		assert.Equal(t, aSchema, schema.Schema())
		assert.Equal(t, aSchemaType, *schema.SchemaType())
		assert.Equal(t, aVersion, schema.Version())
		assert.Equal(t, references, schema.References())
		assert.Equal(t, mockCodec, schema.Codec())
		assert.Equal(t, jsonSchema, schema.JsonSchema())
	}
}

func TestSchemaRequestMarshal(t *testing.T) {
	tests := map[string]struct{
		schema string
		schemaType SchemaType
		references []Reference
		expected string
	}{
		"avro": {
			schema: `test2`,
			schemaType: Avro,
			expected: `{"schema":"test2"}`,
		},
		"protobuf": {
			schema: `test2`,
			schemaType: Protobuf,
			expected: `{"schema":"test2","schemaType":"PROTOBUF"}`,
		},
		"json": {
			schema: `test2`,
			schemaType: Json,
			expected: `{"schema":"test2","schemaType":"JSON"}`,
		},
		"avro-empty-ref": {
			schema: `test2`,
			schemaType: Avro,
			references: make([]Reference, 0),
			expected: `{"schema":"test2"}`,
		},
		"protobuf-empty-ref": {
			schema: `test2`,
			schemaType: Protobuf,
			references: make([]Reference, 0),
			expected: `{"schema":"test2","schemaType":"PROTOBUF"}`,
		},
		"json-empty-ref": {
			schema: `test2`,
			schemaType: Json,
			references: make([]Reference, 0),
			expected: `{"schema":"test2","schemaType":"JSON"}`,
		},
		"avro-ref": {
			schema: `test2`,
			schemaType: Avro,
			references: []Reference{{Name: "name1", Subject: "subject1", Version: 1}},
			expected: `{"schema":"test2","references":[{"name":"name1","subject":"subject1","version":1}]}`,
		},
		"protobuf-ref": {
			schema: `test2`,
			schemaType: Protobuf,
			references: []Reference{{Name: "name1", Subject: "subject1", Version: 1}},
			expected: `{"schema":"test2","schemaType":"PROTOBUF","references":[{"name":"name1","subject":"subject1","version":1}]}`,
		},
		"json-ref": {
			schema: `test2`,
			schemaType: Json,
			references: []Reference{{Name: "name1", Subject: "subject1", Version: 1}},
			expected: `{"schema":"test2","schemaType":"JSON","references":[{"name":"name1","subject":"subject1","version":1}]}`,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			schemaReq := schemaRequest{
				Schema: testData.schema,
				SchemaType: testData.schemaType.String(),
				References: testData.references,
			}
			actual, err := json.Marshal(schemaReq)
			assert.NoError(t, err)
			assert.Equal(t, testData.expected, string(actual))
		})
	}
}

func mockServerFromSubjectVersionPairWithSchemaResponse(t *testing.T, subject, version string, schemaResponse schemaResponse) (*httptest.Server, *int) {
	return mockServerWithSchemaResponse(t, fmt.Sprintf("/subjects/%s/versions/%s", subject, version), schemaResponse)
}

func mockServerFromIDWithSchemaResponse(t *testing.T, id int, schemaResponse schemaResponse) (*httptest.Server, *int) {
	return mockServerWithSchemaResponse(t, fmt.Sprintf("/schemas/ids/%d", id), schemaResponse)
}

func mockServerWithSchemaResponse(t *testing.T, url string, schemaResponse schemaResponse) (*httptest.Server, *int) {
	var count int
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		count++
		response, _ := json.Marshal(schemaResponse)

		switch req.URL.String() {
		case url:
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
