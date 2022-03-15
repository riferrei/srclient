package srclient

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
We are going to test the client meant to be used for testing
*/

var srClient MockSchemaRegistryClient
var schema = "{  " +
	"\"type\": \"record\",  " +
	"\"namespace\": \"com.mycorp.mynamespace\",  " +
	"\"name\": \"value_cdc_fake_2\", " +
	"\"doc\": \"Sample schema to help you get started.\", " +
	"\"fields\": [    " +
	"{   " +
	"\"name\": \"aField\"," +
	"\"type\": \"int\", " +
	"\"doc\": \"The int type is a 32-bit signed integer.\"   " +
	"}" +
	"]" +
	"}"

var schema2 = "{  " +
	"\"type\": \"record\",  " +
	"\"namespace\": \"com.mycorp.mynamespace\",  " +
	"\"name\": \"value_cdc_fake_2\", " +
	"\"doc\": \"Sample schema to help you get started.\", " +
	"\"fields\": [    " +
	"{   " +
	"\"name\": \"bField\"," +
	"\"type\": \"int\", " +
	"\"doc\": \"The int type is a 32-bit signed integer.\"   " +
	"}" +
	"]" +
	"}"

var schema3 = "\"string\""
var schema4 = "\"int\""

/*
We will use init to register some schemas to run our test with.
Due to this, the function that tests MockSchemaRegistryClient.CreateSchema will actually just assert that
the values creates by init are correct and expected.
*/
func init() {
	srClient = CreateMockSchemaRegistryClient("mock://testingUrl")

	// Test Schema and Value Schema creation
	_, _ = srClient.CreateSchema("test1-value", schema, Avro)
	_, _ = srClient.CreateSchema("test1-key", schema, Avro)
	// Test version upgrades for key and value and more registration
	_, _ = srClient.CreateSchema("test1-value", schema2, Avro)
	_, _ = srClient.CreateSchema("test1-key", schema2, Avro)

	// Test version upgrades for key and value and more registration (arbitrary subject)
	_, _ = srClient.CreateSchema("test1_arb", schema3, Avro)
	_, _ = srClient.CreateSchema("test1_arb", schema4, Avro)
}

func TestMockSchemaRegistryClient_CreateSchema(t *testing.T) {

	/*
	 Assert Schemas are registered with proper IDs and Versions
	 By virtue of this test, we also test MockSchemaRegistryClient.GetSchema
	*/
	schemaReg1, _ := srClient.GetSchema(1)
	assert.Equal(t, schema, schemaReg1.schema)
	assert.Equal(t, 1, schemaReg1.version)
	schemaReg2, _ := srClient.GetSchema(2)
	assert.Equal(t, schema, schemaReg2.schema)
	assert.Equal(t, 1, schemaReg2.version)
	schemaReg3, _ := srClient.GetSchema(3)
	assert.Equal(t, schema2, schemaReg3.schema)
	assert.Equal(t, 2, schemaReg3.version)
	schemaReg4, _ := srClient.GetSchema(4)
	assert.Equal(t, schema2, schemaReg4.schema)
	assert.Equal(t, 2, schemaReg4.version)
	schemaReg5, _ := srClient.GetSchema(5)
	assert.Equal(t, schema3, schemaReg5.schema)
	assert.Equal(t, 1, schemaReg5.version)
	schemaReg6, _ := srClient.GetSchema(6)
	assert.Equal(t, schema4, schemaReg6.schema)
	assert.Equal(t, 2, schemaReg6.version)

	// Test registering already registered schema
	_, err := srClient.CreateSchema("test1-key", schema, Avro)
	assert.EqualError(t, err, "POST \"mock://testingUrl/subjects/test1-key/versions\": Schema already registered with id 2")

	// Test registering already registered schema
	_, err = srClient.CreateSchema("test1_arb", schema3, Avro)
	assert.EqualError(t, err, "POST \"mock://testingUrl/subjects/test1_arb/versions\": Schema already registered with id 5")
}

func TestMockSchemaRegistryClient_GetLatestSchema(t *testing.T) {

	latest, err := srClient.GetLatestSchema("test1-key")
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
	} else {
		assert.Equal(t, schema2, latest.schema)
	}

	latest, err = srClient.GetLatestSchema("test1_arb")
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
	} else {
		assert.Equal(t, schema4, latest.schema)
	}
}

func TestMockSchemaRegistryClient_GetLatestSchema_WhenNoSchemaRegistered(t *testing.T) {
	localClient := CreateMockSchemaRegistryClient("")
	var err error
	assert.NotPanics(t, func() {
		_, err = localClient.GetLatestSchema("some-innexistent-schema-subject")
	})
	assert.EqualError(t, err, "Subject not found")
}

func TestMockSchemaRegistryClient_GetSchemaVersions(t *testing.T) {
	versions, _ := srClient.GetSchemaVersions("test1-key")
	assert.Equal(t, 2, len(versions))

	versions, _ = srClient.GetSchemaVersions("test1_arb")
	assert.Equal(t, 2, len(versions))
}

func TestMockSchemaRegistryClient_GetSchemaByVersion(t *testing.T) {
	oldVersion, _ := srClient.GetSchemaByVersion("test1-value", 1)
	assert.Equal(t, schema, oldVersion.schema)

	oldVersion, _ = srClient.GetSchemaByVersion("test1_arb", 1)
	assert.Equal(t, schema3, oldVersion.schema)
}

func TestMockSchemaRegistryClient_GetSubjects(t *testing.T) {
	allSubjects, _ := srClient.GetSubjects()
	sort.Strings(allSubjects)
	assert.Equal(t, allSubjects, []string{"test1-key", "test1-value", "test1_arb"})
}

func TestMockSchemaRegistryClient_DeleteSubjectByVersion(t *testing.T) {
	originalCount, _ := srClient.GetSchemaVersions("test1-value")
	err := srClient.DeleteSubjectByVersion("test1-value", 1, true)
	assert.NoError(t, err)
	newCount, _ := srClient.GetSchemaVersions("test1-value")
	assert.Equal(t, len(originalCount)-1, len(newCount))
}
