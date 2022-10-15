package srclient

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	avroType = Avro
	protobuf = Protobuf
)

var (
	testSchema1 = `{"type": "record", "name": "cupcake", "fields": [{"name": "flavor", "type": "string"}]}`
	testSchema2 = `{"type": "record", "name": "bakery", "fields": [{"name": "number", "type": "int"}]}`
)

func TestMockSchemaRegistryClient_CreateSchema_RegistersSchemaCorrectly(t *testing.T) {
	tests := map[string]struct {
		subject    string
		schema     string
		schemaType SchemaType

		currentIdCounter      int
		existingSchemaCounter int

		expectedSchema *Schema
	}{
		"new avro schema": {
			subject:    "cupcake",
			schema:     testSchema1,
			schemaType: Avro,

			currentIdCounter:      1,
			existingSchemaCounter: 0,

			expectedSchema: &Schema{
				id:         2,
				version:    1,
				schemaType: &avroType,
				schema:     testSchema1,
			},
		},
		"existing avro schema": {
			subject:    "bakery",
			schema:     testSchema2,
			schemaType: Avro,

			currentIdCounter:      6,
			existingSchemaCounter: 10,

			expectedSchema: &Schema{
				id:         7,
				version:    11,
				schemaType: &avroType,
				schema:     testSchema2,
			},
		},
		"new protobuf schema": {
			subject:    "bakery",
			schema:     testSchema2,
			schemaType: protobuf,

			currentIdCounter:      23,
			existingSchemaCounter: 75,

			expectedSchema: &Schema{
				id:         24,
				version:    76,
				schemaType: &protobuf,
				schema:     testSchema2,
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			registry := CreateMockSchemaRegistryClient("http://localhost:8081")
			registry.idCounter = testData.currentIdCounter

			// Add existing schemas
			if testData.existingSchemaCounter > 0 {
				registry.schemaCache[testData.subject] = map[int]*Schema{}
				for i := 1; i <= testData.existingSchemaCounter; i++ {
					registry.schemaCache[testData.subject][i] = new(Schema)
				}
			}

			// Act
			schema, err := registry.CreateSchema(testData.subject, testData.schema, testData.schemaType)

			// Assert
			if assert.Nil(t, err) {
				assert.Equal(t, testData.expectedSchema.id, schema.id)
				assert.Equal(t, testData.expectedSchema.version, schema.version)
				assert.Equal(t, testData.expectedSchema.schemaType, schema.schemaType)
				assert.Equal(t, testData.expectedSchema.schema, schema.schema)

				assert.Equal(t, schema, registry.idCache[schema.id])
				assert.Equal(t, schema, registry.schemaCache[testData.subject][schema.version])
			}
		})
	}
}

func TestMockSchemaRegistryClient_SetSchema_RegistersSchemaCorrectly(t *testing.T) {
	tests := map[string]struct {
		subject    string
		schema     string
		schemaType SchemaType
		id         int
		version    int

		existingSchemaCounter int

		expectedSchema *Schema
	}{
		"new avro schema": {
			subject:    "cupcake",
			schema:     testSchema1,
			schemaType: Avro,
			id:         52,
			version:    -1,

			existingSchemaCounter: 0,

			expectedSchema: &Schema{
				id:         52,
				version:    1,
				schemaType: &avroType,
				schema:     testSchema1,
			},
		},
		"existing avro schema": {
			subject:    "bakery",
			schema:     testSchema2,
			schemaType: Avro,
			id:         7,
			version:    -1,

			existingSchemaCounter: 10,

			expectedSchema: &Schema{
				id:         7,
				version:    11,
				schemaType: &avroType,
				schema:     testSchema2,
			},
		},
		"new protobuf schema": {
			subject:    "bakery",
			schema:     testSchema2,
			schemaType: Protobuf,
			id:         24,
			version:    -1,

			existingSchemaCounter: 75,

			expectedSchema: &Schema{
				id:         24,
				version:    76,
				schemaType: &protobuf,
				schema:     testSchema2,
			},
		},
		"with given version": {
			subject:    "bakery",
			schema:     testSchema2,
			schemaType: Avro,
			id:         7,
			version:    634,

			expectedSchema: &Schema{
				id:         7,
				version:    634,
				schemaType: &avroType,
				schema:     testSchema2,
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			registry := CreateMockSchemaRegistryClient("http://localhost:8081")

			// Add existing schemas
			if testData.existingSchemaCounter > 0 {
				registry.schemaCache[testData.subject] = map[int]*Schema{}
				for i := 1; i <= testData.existingSchemaCounter; i++ {
					registry.schemaCache[testData.subject][i] = new(Schema)
				}
			}

			// Act
			schema, err := registry.SetSchema(testData.id, testData.subject, testData.schema, testData.schemaType, testData.version)

			// Assert
			if assert.Nil(t, err) {
				assert.Equal(t, testData.expectedSchema.id, schema.id)
				assert.Equal(t, testData.expectedSchema.version, schema.version)
				assert.Equal(t, testData.expectedSchema.schemaType, schema.schemaType)
				assert.Equal(t, testData.expectedSchema.schema, schema.schema)

				assert.Equal(t, schema, registry.idCache[schema.id])
				assert.Equal(t, schema, registry.schemaCache[testData.subject][schema.version])
			}
		})
	}
}

func TestMockSchemaRegistryClient_SetSchema_CorrectlyUpdatesIdCounter(t *testing.T) {
	tests := map[string]struct {
		currentId  int
		newId      int
		expectedId int
	}{
		"0 to 1": {
			currentId:  0,
			newId:      1,
			expectedId: 1,
		},
		"5 to 19": {
			currentId:  5,
			newId:      19,
			expectedId: 19,
		},
		"no change": {
			currentId:  9,
			newId:      2,
			expectedId: 9,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			registry := CreateMockSchemaRegistryClient("http://localhost:8081")
			registry.idCounter = testData.currentId

			// Act
			_, _ = registry.SetSchema(testData.newId, "cupcake", `{}`, Avro, 0)

			// Assert
			assert.Equal(t, testData.expectedId, registry.idCounter)
		})
	}
}

func TestMockSchemaRegistryClient_CreateSchema_ReturnsErrorOnInvalidSchemaType(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	schema, err := registry.CreateSchema("", "", "random")

	// Assert
	assert.Nil(t, schema)
	assert.Equal(t, errInvalidSchemaType, err)
}

func TestMockSchemaRegistryClient_CreateSchema_ReturnsErrorOnDuplicateSchema(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	registry.schemaCache["cupcake"] = map[int]*Schema{
		23: {
			schema: "{}",
		},
	}

	// Act
	schema, err := registry.CreateSchema("cupcake", "{}", avroType)

	// Assert
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, errSchemaAlreadyRegistered)
}

func TestMockSchemaRegistryClient_GetSchema_ReturnsSchema(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	schema := &Schema{}

	registry.idCache = map[int]*Schema{
		234: schema,
	}

	// Act
	result, err := registry.GetSchema(234)

	// Assert
	assert.Nil(t, err)
	assert.Same(t, schema, result)
}

func TestMockSchemaRegistryClient_GetSchema_ReturnsErrOnNotFound(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetSchema(234)

	// Assert
	assert.ErrorIs(t, err, errSchemaNotFound)

	assert.Nil(t, result)
}

func TestMockSchemaRegistryClient_GetLatestSchema_ReturnsErrorOn0SchemaVersions(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetLatestSchema("cupcake")

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errSchemaNotFound)
}

func TestMockSchemaRegistryClient_GetLatestSchema_ReturnsExpectedSchema(t *testing.T) {
	tests := map[string]struct {
		subject         string
		existingSchemas map[int]*Schema
		expectedSchema  *Schema
	}{
		"cupcake": {
			subject:         "cupcake",
			existingSchemas: map[int]*Schema{23: {id: 23}},
			expectedSchema:  &Schema{id: 23},
		},
		"bakery": {
			subject: "bakery",
			existingSchemas: map[int]*Schema{
				1: {id: 1},
				2: {id: 2},
				5: {id: 5},
			},
			expectedSchema: &Schema{id: 5},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			registry := CreateMockSchemaRegistryClient("http://localhost:8081")
			registry.schemaCache[testData.subject] = testData.existingSchemas

			// Act
			result, err := registry.GetLatestSchema(testData.subject)

			// Assert
			assert.Nil(t, err)

			assert.Equal(t, testData.expectedSchema.id, result.id)
		})
	}
}

func TestMockSchemaRegistryClient_GetSchemaVersions_ReturnsSchemaVersions(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache["cupcake"] = map[int]*Schema{
		1: {id: 1},
		2: {id: 2},
		3: {id: 3},
	}

	// Act
	result, err := registry.GetSchemaVersions("cupcake")

	// Assert
	assert.Nil(t, err)

	assert.Equal(t, []int{1, 2, 3}, result)
}

func TestMockSchemaRegistryClient_GetSchemaByVersion_ReturnsErrorOnSubjectNotFound(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetSchemaByVersion("cupcake", 0)

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errSubjectNotFound)
}

func TestMockSchemaRegistryClient_GetSchemaByVersion_ReturnsErrorOnSchemaNotFound(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache = map[string]map[int]*Schema{
		"cupcake": {},
	}

	// Act
	result, err := registry.GetSchemaByVersion("cupcake", 0)

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errSchemaNotFound)
}

func TestMockSchemaRegistryClient_GetSchemaByVersion_ReturnsSchema(t *testing.T) {
	tests := map[string]struct {
		subject string
		version int

		existingSchemas map[int]*Schema
		expectedSchema  *Schema
	}{
		"cupcake": {
			subject: "cupcake",
			version: 23,

			existingSchemas: map[int]*Schema{23: {id: 1}},
			expectedSchema:  &Schema{id: 1},
		},
		"bakery": {
			subject: "bakery",
			version: 2,

			existingSchemas: map[int]*Schema{
				1: {id: 4},
				2: {id: 5},
				5: {id: 6},
			},
			expectedSchema: &Schema{id: 5},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			registry := CreateMockSchemaRegistryClient("http://localhost:8081")
			registry.schemaCache[testData.subject] = testData.existingSchemas

			// Act
			result, err := registry.GetSchemaByVersion(testData.subject, testData.version)

			// Assert
			assert.Nil(t, err)

			assert.Equal(t, testData.expectedSchema.id, result.id)
		})
	}
}

func TestMockSchemaRegistryClient_GetSubjects_ReturnsAllSubjects(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache = map[string]map[int]*Schema{
		"1": {},
		"2": {},
		"3": {},
	}

	// Act
	result, err := registry.GetSubjects()

	// Assert
	assert.Nil(t, err)
	assert.Contains(t, result, "1")
	assert.Contains(t, result, "2")
	assert.Contains(t, result, "3")
}

func TestMockSchemaRegistryClient_GetSubjectsIncludingDeleted_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetSubjectsIncludingDeleted()

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}

func TestMockSchemaRegistryClient_DeleteSubject_DeletesSubject(t *testing.T) {
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache = map[string]map[int]*Schema{
		"b": {},
	}

	// Act
	err := registry.DeleteSubject("b", false)

	// Assert
	assert.Nil(t, err)
	assert.Equal(t, map[string]map[int]*Schema{}, registry.schemaCache)
}

func TestMockSchemaRegistryClient_DeleteSubjectByVersion_DeletesSubjectVersion(t *testing.T) {
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache = map[string]map[int]*Schema{
		"b": {
			1: {id: 1},
			2: {id: 2},
			3: {id: 3},
		},
	}

	// Act
	err := registry.DeleteSubjectByVersion("b", 2, false)

	// Assert
	assert.Nil(t, err)
	if assert.NotNil(t, registry.schemaCache["b"]) {
		assert.Nil(t, registry.schemaCache["b"][2])
	}
}

func TestMockSchemaRegistryClient_DeleteSubjectByVersion_ReturnsErrorOnSubjectNotFound(t *testing.T) {
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	err := registry.DeleteSubjectByVersion("cupcake", 5, false)

	// Assert
	assert.ErrorIs(t, err, errSubjectNotFound)
}

func TestMockSchemaRegistryClient_DeleteSubjectByVersion_ReturnsErrorOnVersionNotFound(t *testing.T) {
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.schemaCache = map[string]map[int]*Schema{
		"cupcake": {
			1: {id: 1},
			3: {id: 3},
		},
	}

	// Act
	err := registry.DeleteSubjectByVersion("cupcake", 5, false)

	// Assert
	assert.ErrorIs(t, err, errSchemaNotFound)
}

func TestMockSchemaRegistryClient_ChangeSubjectCompatibilityLevel_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.ChangeSubjectCompatibilityLevel("", "")

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}

func TestMockSchemaRegistryClient_GetGlobalCompatibilityLevel_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetGlobalCompatibilityLevel()

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}

func TestMockSchemaRegistryClient_GetCompatibilityLevel_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.GetCompatibilityLevel("", false)

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}

func TestMockSchemaRegistryClient_IsSchemaCompatible_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.IsSchemaCompatible("", "", "", "")

	// Assert
	assert.False(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}

func TestMockSchemaRegistryClient_LookupSchema_IsNotImplemented(t *testing.T) {
	// Arrange
	registry := CreateMockSchemaRegistryClient("http://localhost:8081")

	// Act
	result, err := registry.LookupSchema("", "", "")

	// Assert
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errNotImplemented)
}
