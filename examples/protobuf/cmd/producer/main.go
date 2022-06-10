package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/riferrei/srclient/examples/protobuf/lib"
	pb "github.com/riferrei/srclient/examples/protobuf/proto"
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {
	brokers := getenv("BROKERS", "kafkasvc:9092")
	topic := getenv("TOPIC", "my-topic")
	schemaRegistryUrl := getenv("SR_URL", "http://schemaregistry:8081")

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	schemaRegistryClient := lib.CreateSchemaRegistryClient(schemaRegistryUrl)

	// srProtoResolver := lib.NewSchemaRegistryProtobufResolver(schemaRegistryClient, protoregistry.GlobalTypes, lib.ValueDeserialization)
	topicNameResolver := lib.NewTopicNameSchemaResolver(schemaRegistryClient, lib.ValueSerialization)

	// valueDeserializer := lib.NewProtobufDeserializer(srProtoResolver)
	valueSerializer := lib.NewProtobufSerializer(topicNameResolver, nil)

	cfg := sarama.NewConfig()
	cfg.ClientID = "producer-example"
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Metadata.Full = true
	cfg.Version = sarama.V1_0_0_0
	cfg.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), cfg)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}

	admin, err := sarama.NewClusterAdmin(strings.Split(brokers, ","), cfg)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	defer func() { _ = admin.Close() }()

	// Create our topic
	topics, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		log.Fatal("Error while describing topics: ", err.Error())
	}
	if len(topics) == 0 {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Fatal("Error while creating topic: ", err.Error())
		}
	}

	// Add the schema to registry
	if _, err := topicNameResolver.ResolveSchema(topic); err != nil {
		schema, err := ioutil.ReadFile("hello.proto")
		if err != nil {
			log.Fatal("Error while reading proto: ", err.Error())
		}
		_, err = schemaRegistryClient.CreateSchema(topic+"-value", string(schema), srclient.Protobuf)
		if err != nil {
			log.Fatal("Error while creating schema: ", err.Error())
		}
	}

	for {
		time.Sleep(time.Second)
		ctx := context.TODO()

		val, err := valueSerializer.Serialize(ctx, topic, &pb.HelloRecord{
			Name: "my name",
			Time: timestamppb.New(time.Now()),
		})
		if err != nil {
			log.Println("Error while serializing message:", err.Error())
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(val),
		}

		partition, offset, err := syncProducer.SendMessage(msg)
		if err != nil {
			log.Println("Error while writing message:", err.Error())
			continue
		}
		log.Printf("Wrote to partition %d offset %d.", partition, offset)
	}
}
