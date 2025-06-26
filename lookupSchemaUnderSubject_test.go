package srclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupSchemaUnderSubject_Success(t *testing.T) {
	// Mock server that returns a successful lookup response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request method and path
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/test-subject", r.URL.Path)

		// Verify query parameters
		params := r.URL.Query()
		assert.Equal(t, "true", params.Get("normalize"))
		assert.Empty(t, params.Get("format"))

		// Verify content type
		assert.Equal(t, contentType, r.Header.Get("Content-Type"))

		// Verify request body
		var reqBody RegisterSchemaRequest
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		require.NoError(t, err)
		assert.Equal(t, `{"type":"string"}`, reqBody.Schema)
		assert.Equal(t, Avro, reqBody.SchemaType)

		// Return successful response
		response := lookupSchemaResponse{
			Schema:  `{"type": "string"}`, // Normalized schema
			ID:      123,
			Version: 1,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
		References: []Reference{},
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, true)

	require.NoError(t, err)
	assert.Equal(t, 1, version)
	assert.Equal(t, 123, globalID)
	assert.Equal(t, `{"type": "string"}`, schemaStr)
}

func TestLookupSchemaUnderSubject_NoNormalize(t *testing.T) {
	// Mock server that returns a successful lookup response without normalization
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify query parameters - normalize should not be present or be false
		params := r.URL.Query()
		normalizeParam := params.Get("normalize")
		assert.True(t, normalizeParam == "" || normalizeParam == "false")

		// Return successful response
		response := lookupSchemaResponse{
			Schema:  `{"type":"string"}`,
			ID:      456,
			Version: 2,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, false)

	require.NoError(t, err)
	assert.Equal(t, 2, version)
	assert.Equal(t, 456, globalID)
	assert.Equal(t, `{"type":"string"}`, schemaStr)
}

func TestLookupSchemaUnderSubject_SchemaNotFound(t *testing.T) {
	// Mock server that returns 40403 error (schema not found)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)

		errorResponse := map[string]interface{}{
			"error_code": 40403,
			"message":    "Schema not found",
		}
		json.NewEncoder(w).Encode(errorResponse)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"unknown"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, true)

	// Should return semantic schema not found
	assert.ErrorIs(t, err, ErrSemanticSchemaNotFound)
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}

func TestLookupSchemaUnderSubject_SubjectNotFound(t *testing.T) {
	// Mock server that returns 40401 error (subject not found)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)

		errorResponse := map[string]interface{}{
			"error_code": 40401,
			"message":    "Subject not found",
		}
		json.NewEncoder(w).Encode(errorResponse)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "nonexistent-subject", req, true)

	// Should return a general error (not the semantic schema not found error)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSemanticSchemaNotFound)
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}

func TestLookupSchemaUnderSubject_ServerError(t *testing.T) {
	// Mock server that returns 500 error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)

		errorResponse := map[string]interface{}{
			"error_code": 50001,
			"message":    "Internal server error",
		}
		json.NewEncoder(w).Encode(errorResponse)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to lookup schema")
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}

func TestLookupSchemaUnderSubject_NilRequest(t *testing.T) {
	client := NewSchemaRegistryClient("http://test.com")
	ctx := context.Background()

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", nil, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "RegisterSchemaRequest cannot be nil")
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}

func TestLookupSchemaUnderSubject_ContextCancellation(t *testing.T) {
	// Create a slow server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(lookupSchemaResponse{
			Schema:  `{"type":"string"}`,
			ID:      123,
			Version: 1,
		})
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}

func TestLookupSchemaUnderSubject_WithReferences(t *testing.T) {
	// Mock server that handles requests with references
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body includes references
		var reqBody RegisterSchemaRequest
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		require.NoError(t, err)

		assert.Equal(t, `{"type":"record","name":"Test","fields":[{"name":"ref","type":"RefType"}]}`, reqBody.Schema)
		assert.Equal(t, Json, reqBody.SchemaType)
		assert.Len(t, reqBody.References, 1)
		assert.Equal(t, "RefType", reqBody.References[0].Name)
		assert.Equal(t, "ref-subject", reqBody.References[0].Subject)
		assert.Equal(t, 1, reqBody.References[0].Version)

		// Return successful response
		response := lookupSchemaResponse{
			Schema:  `{"type":"record","name":"Test","fields":[{"name":"ref","type":"RefType"}]}`,
			ID:      789,
			Version: 3,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"record","name":"Test","fields":[{"name":"ref","type":"RefType"}]}`,
		SchemaType: Json,
		References: []Reference{
			{
				Name:    "RefType",
				Subject: "ref-subject",
				Version: 1,
			},
		},
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, false)

	require.NoError(t, err)
	assert.Equal(t, 3, version)
	assert.Equal(t, 789, globalID)
	assert.Equal(t, `{"type":"record","name":"Test","fields":[{"name":"ref","type":"RefType"}]}`, schemaStr)
}

func TestLookupSchemaUnderSubject_InvalidJSON(t *testing.T) {
	// Mock server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL)
	ctx := context.Background()

	req := &RegisterSchemaRequest{
		Schema:     `{"type":"string"}`,
		SchemaType: Avro,
	}

	version, globalID, schemaStr, err := client.LookupSchemaUnderSubject(ctx, "test-subject", req, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal response")
	assert.Equal(t, 0, version)
	assert.Equal(t, 0, globalID)
	assert.Equal(t, "", schemaStr)
}
