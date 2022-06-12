// +build integration

package srclient

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var srclientUrlEnvName = "SRCLIENT_URL"
var srclientUrl string = os.Getenv(srclientUrlEnvName)
var client *SchemaRegistryClient = CreateSchemaRegistryClient(srclientUrl)

func TestGetSubjects(t *testing.T) {
	subjects, err := client.GetSubjects()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log("Test passes: ", subjects)
}

func TestCreateSchema(t *testing.T) {
	subject := "LongList"
	schema := `{
		"type": "record",
		"name": "LongList",
		"aliases": ["LinkedLongs"],
		"fields" : [
		  {"name": "value", "type": "long"},
		  {"name": "next", "type": ["null", "LongList"]}
		]
	  }`

	_, err := client.CreateSchema(subject, schema, Avro)
	if err != nil {
		t.Fatal(err.Error())
	}

	subjects, err := client.GetSubjects()
	if err != nil {
		t.Fatal(err.Error())
	}

	assert.Contains(t, subjects, subject)
}
