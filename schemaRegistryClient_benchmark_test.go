package srclient

import "testing"

// variables required for testing with a real schema registry
// set them to your local schema registry test url and topic
// TODO perhaps CI for this
var (
	schemaRegistryURL string = "http://192.168.8.38:8081"
	topic             string = "APMAN_systems_VirtMem"
)

// BenchmarkGetLatestSchema both tests and benchmarks the said function
func BenchmarkGetLatestSchema(b *testing.B) {
	if schemaRegistryURL == "" || topic == "" {
		b.Skip("url or topic is empty")
	}
	srclient := CreateSchemaRegistryClient(schemaRegistryURL)
	srclient.SetCachingEnabled(true)
	srclient.SetCodecCreationEnabled(true)

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

func BenchmarkGetSchemaBySubject(b *testing.B) {
	if schemaRegistryURL == "" || topic == "" {
		b.Skip("url or topic is empty")
	}
	srclient := CreateSchemaRegistryClient(schemaRegistryURL)
	srclient.SetCachingEnabled(true)
	srclient.SetCodecCreationEnabled(true)

	b.Run("BenchmarkGetSchemaBySubjectWithCachingEnabled", GenerateBenchmarkGetSchemaBySubject(srclient))

	// srclient.SetCachingEnabled(false)

	// b.Run("BenchmarkGetSchemaBySubjectWithCachingDisabled", GenerateBenchmarkGetSchemaBySubject(srclient))
}

func GenerateBenchmarkGetSchemaBySubject(srclient *SchemaRegistryClient) func(*testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			schema, err := srclient.GetSchemaBySubject(topic, false)
			b.StopTimer()
			if err != nil {
				b.Fatal(err.Error())
			}
			_ = schema
			b.StartTimer()
		}
	}
}

// // BenchmarkGetLatestSchemaFromCache both tests and benchmarks the said function
// func BenchmarkGetLatestSchemaFromCache(b *testing.B) {
// 	if schemaRegistryURL == "" || topic == "" {
// 		b.Skip("url or topic is empty")
// 	}
// 	srclient := CreateSchemaRegistryClient(schemaRegistryURL)
// 	srclient.CachingEnabled(true)
// 	srclient.CodecCreationEnabled(true)
// 	schema, err := srclient.GetLatestSchema(topic, false)
// 	if err != nil {
// 		b.Fatal(err.Error())
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		schemaFromCache, err := srclient.GetLatestSchemaFromCache(topic, false)
// 		b.StopTimer()
// 		if err != nil {
// 			b.Fatal(err.Error())
// 		}
// 		if schema.Schema() != schemaFromCache.Schema() {
// 			b.Fatal("schemas are not equal")
// 		}
// 		b.StartTimer()
// 	}
// }
