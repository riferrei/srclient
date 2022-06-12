# Schema Registry Client for Go

 [![Go Report Card](https://goreportcard.com/badge/github.com/riferrei/srclient)](https://goreportcard.com/report/github.com/riferrei/srclient) [![Go Reference](https://pkg.go.dev/badge/github.com/riferrei/srclient.svg)](https://pkg.go.dev/github.com/riferrei/srclient)

<img src="https://github.com/riferrei/srclient/raw/master/images/Gopher_Dropping_Mic.png" width="150" height="150">

**srclient** is a Golang client for [Schema Registry](https://www.confluent.io/confluent-schema-registry/), a software that provides a RESTful interface for developers to define standard schemas for their events, share them across the organization, and safely evolve them in a way that is backward compatible and future proof.
Using this client allows developers to build Golang programs that write and read schema compatible records to/from [Apache Kafka](https://kafka.apache.org/) using [Avro](https://avro.apache.org/), [Protobuf](https://developers.google.com/protocol-buffers), and [JSON Schemas](https://json-schema.org) while Schema Registry is used to manage the schemas used.
Using this architecture, producers programs interact with Schema Registry to retrieve schemas and use it to serialize records. Then consumer programs can retrieve the same schema from Schema Registry to deserialize the records.
You can read more about the benefits of using Schema Registry [here](https://www.confluent.io/blog/schemas-contracts-compatibility).

## Features

* **Simple to Use** - This client provides a very high-level abstraction over the operations developers writing programs for Apache Kafka typically need.
Thus, it will feel natural for them to use this client's functions.
Moreover, developers don't need to handle low-level HTTP details to communicate with Schema Registry.
* **Performance** - This client provides caching capabilities.
This means that any data retrieved from Schema Registry can be cached locally to improve the performance of subsequent requests.
This allows programs not co-located with Schema Registry to reduce the latency necessary on each request.
This functionality can be disabled programmatically.

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Installation

Module install:

This client is a Go module, therefore you can have it simply by adding the following import to your code:

```go
import "github.com/riferrei/srclient"
```

Then run a build to have this client automatically added to your go.mod file as a dependency.

Manual install:

```bash
go get -u github.com/riferrei/srclient
```

## Testing

Unit testing can be run with the generic go test command:

```bash
go test -cover -v ./...
```

You can also run integration testing in your local machine given you have docker installed:

```bash
docker compose up --exit-code-from srclient-integration-test
docker compose down --rmi local
```

## Getting Started & Examples

* [Package documentation](https://pkg.go.dev/github.com/riferrei/srclient) is a good place to start
* For Avro examples see [EXAMPLES_AVRO.md](EXAMPLES_AVRO.md)
* For Protobuf examples, see [EXAMPLES_PROTOBUF.md](EXAMPLES_PROTOBUF.md)
* Consult [Confluent's Schema Registry documentation](https://docs.confluent.io/platform/current/schema-registry/index.html) for details


## Acknowledgements
* Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/).
* The [Go Gopher](https://blog.golang.org/gopher), is an artistic creation of [Renee French](http://reneefrench.blogspot.com/).