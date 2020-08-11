= Schema Registry Client for Go
:toc:

:imagesdir: images/
image::Gopher_Dropping_Mic.png[Gopher, 150, 150, float="left"]

*srclient* is a Golang client for https://www.confluent.io/confluent-schema-registry/[Schema Registry], a software that provides a RESTful interface for developers to define standard schemas for their events, share them across the organization and safely evolve them in a way that is backward compatible and future proof.
Using this client allows developers to build Golang programs that write and read schema compatible records to/from https://kafka.apache.org/[Apache Kafka] using https://avro.apache.org/[Avro], https://developers.google.com/protocol-buffers[Protobuf], and https://json-schema.org[JSON Schemas] while Schema Registry is used to manage the schemas used.
Using this architecture, producers programs interact with Schema Registry to retrieve schemas and use it to serialize records, and then consumer programs can retrieve the same schema from Schema Registry to deserialize the records.
You can read more about the benefits of using Schema Registry https://www.confluent.io/blog/schemas-contracts-compatibility[here].

== Features

* *Simple to Use* - This client provides a very high-level abstraction over the operations that developers writing programs for Apache Kafka typically need.
Thus, it will feel natural for them using the functions that this client provides.
Moreover, developers don't need to handle low-level HTTP details to communicate with Schema Registry.
* *Performance* - This client provides caching capabilities.
This means that any data retrieved from Schema Registry can be cached locally to improve the performance of subsequent requests.
This allows programs that are not co-located with Schema Registry to reduce the latency necessary on each request.
This functionality can be disabled programmatically.
* *Confluent Cloud* - Go developers using https://www.confluent.io/confluent-cloud/[Confluent Cloud] can use this client to interact with the fully managed Schema Registry, which provides important features like schema enforcement that enable teams to reduce deployment issues by governing the schema changes as they evolve.

*License*: http://www.apache.org/licenses/LICENSE-2.0[Apache License v2.0]

== Installation

Module install:

This client is a Go module, therefore you can have it simply by adding the following import to your code:

[source,golang]
----
import "github.com/riferrei/srclient"
----

Then run a build to have this client automatically added to your go.mod file as a dependency.

Manual install:

[source,bash]
----
go get -u github.com/riferrei/srclient
----

== Examples

.Producer
[source,golang]
----
import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type ComplexType struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {

	topic := "myTopic"

	// 1) Create the producer as you would normally do using Confluent's Go client
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for event := range p.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the message '%s'\n", message.Key)
				} else {
					fmt.Printf("Message '%s' delivered successfully!\n", message.Key)
				}
			}
		}
	}()

	// 2) Fetch the latest version of the schema, or create a new one if it is the first
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schema, err := schemaRegistryClient.GetLatestSchema(topic, false)
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile("complexType.avsc")
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Avro, false)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 3) Serialize the record using the schema provided by the client,
	// making sure to include the schema id as part of the record.
	newComplexType := ComplexType{ID: 1, Name: "Gopher"}
	value, _ := json.Marshal(newComplexType)
	native, _, _ := schema.Codec().NativeFromTextual(value)
	valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	key, _ := uuid.NewUUID()
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(key.String()), Value: recordValue}, nil)

	p.Flush(15 * 1000)

}
----

.Consumer
[source,golang]
----
import (
	"encoding/binary"
	"fmt"

	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	// 1) Create the consumer as you would
	// normally do using Confluent's Go client
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)

	// 2) Create a instance of the client to retrieve the schemas for each message
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			// 3) Recover the schema id from the message and use the
			// client to retrieve the schema from Schema Registry.
			// Then use it to deserialize the record accordingly.
			schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
			schema, err := schemaRegistryClient.GetSchema(int(schemaID))
			if err != nil {
				panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaID, err))
			}
			native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
			value, _ := schema.Codec().TextualFromNative(nil, native)
			fmt.Printf("Here is the message %s\n", string(value))
		} else {
			fmt.Printf("Error consuming the message: %v (%v)\n", err, msg)
		}
	}

	c.Close()
	
}
----

Both examples have been created using https://github.com/confluentinc/confluent-kafka-go[Confluent's Golang for Apache Kafka^TM^].

== Confluent Cloud

To use this client with https://www.confluent.io/confluent-cloud/[Confluent Cloud] you will need the endpoint of your managed Schema Registry and an API Key/Secret.
Both can be easily retrieved from the Confluent Cloud UI once you select an environment:

image::Getting_Endpoint_and_APIKeys.png[]

Finally, your Go program need to provide this information to the client:

[source,golang]
----
schemaRegistryClient := srclient.CreateSchemaRegistryClient("https://prefix.us-east-2.aws.confluent.cloud")
schemaRegistryClient.SetCredentials("apiKey", "apiSecret")
----

== Acknowledgements

* Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the https://www.apache.org/[Apache Software Foundation].
* The https://blog.golang.org/gopher[Go Gopher], is an artistic creation of http://reneefrench.blogspot.com/[Renee French].