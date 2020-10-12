package srclient

import "testing"

// variables required for testing with a real schema registry
// set them to your local schema registry test url and topic
// TODO perhaps CI for this
var (
	schemaRegistryURL string
	topic             string
)

// BenchmarkGetLatestSchema both tests and benchmarks the said function
func BenchmarkGetLatestSchema(b *testing.B) {
	if schemaRegistryURL == "" || topic == "" {
		b.Skip("url or topic is empty")
	}
	srclient := CreateSchemaRegistryClient(schemaRegistryURL)
	srclient.CachingEnabled(true)
	srclient.CodecCreationEnabled(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema, err := srclient.GetLatestSchema(topic, false)
		b.StopTimer()
		if err != nil {
			b.Fatal(err.Error())
		}
		_ = schema
		b.StartTimer()
	}
}

// BenchmarkGetLatestSchemaFromCache both tests and benchmarks the said function
func BenchmarkGetLatestSchemaFromCache(b *testing.B) {
	if schemaRegistryURL == "" || topic == "" {
		b.Skip("url or topic is empty")
	}
	srclient := CreateSchemaRegistryClient(schemaRegistryURL)
	srclient.CachingEnabled(true)
	srclient.CodecCreationEnabled(true)
	schema, err := srclient.GetLatestSchema(topic, false)
	if err != nil {
		b.Fatal(err.Error())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schemaFromCache, err := srclient.GetLatestSchemaFromCache(topic, false)
		b.StopTimer()
		if err != nil {
			b.Fatal(err.Error())
		}
		if schema.Schema() != schemaFromCache.Schema() {
			b.Fatal("schemas are not equal")
		}
		b.StartTimer()
	}
}
