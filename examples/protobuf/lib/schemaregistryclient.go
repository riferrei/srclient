package lib

import (
	"context"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/riferrei/srclient"
)

// ErrNonProtobufSchema means the registered type of the schema in the Schema Registry was not a Protobuf
var ErrNonProtobufSchema = errors.New("schema is not a Protobuf schema")

// ErrInvalidSchema means that multiple FileDescriptors were returned when parsing the schema
var ErrInvalidSchema = errors.New("unexpected schema from schema registry")

// Simplified interface for srclient.SchemaRegistryClient
type iSchemaRegistryClient interface {
	GetSchema(schemaID int) (*srclient.Schema, error)
	GetProtoSchema(ctx context.Context, schemaID int) (*desc.FileDescriptor, error)
	GetLatestSchema(subject string) (*srclient.Schema, error)
	SetCredentials(username string, password string)
}

// protoCache caches the parsed Protobuf schema
type protoCache struct {
	sync.RWMutex
	m map[int]*desc.FileDescriptor
}

type schemaRegistryClient struct {
	*srclient.SchemaRegistryClient
	protoCache *protoCache
}

func CreateSchemaRegistryClient(schemaRegistryURL string) *schemaRegistryClient {
	return &schemaRegistryClient{
		srclient.CreateSchemaRegistryClient(schemaRegistryURL),
		newProtoCache(),
	}
}

func (s *schemaRegistryClient) GetProtoSchema(ctx context.Context, schemaId int) (*desc.FileDescriptor, error) {
	if fd, ok := s.protoCache.Get(schemaId); ok {
		return fd, nil
	}

	if schemaId == 0 {
		// Sometimes we get really bad data on the topic
		return nil, ErrNonProtobufSchema
	}

	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			log.Println(filename)
			var schema *srclient.Schema
			var err error

			// filename is a schema id, fetch it directly
			if sid, err := strconv.Atoi(filename); err == nil {
				schema, err = s.GetSchema(sid)
			} else {
				// otherwise its likely an import and we look it up by its filename
				schema, err = s.GetLatestSchema(filename)
			}

			if err != nil {
				return nil, err
			}
			spew.Dump(filename, schema)
			if *(schema.SchemaType()) != srclient.Protobuf {
				return nil, ErrNonProtobufSchema
			}
			return io.NopCloser(strings.NewReader(schema.Schema())), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(strconv.Itoa(schemaId))
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, ErrInvalidSchema
	}
	fd := fileDescriptors[0]
	s.protoCache.Put(schemaId, fd)

	return fd, nil
}

func newProtoCache() *protoCache {
	return &protoCache{
		m: make(map[int]*desc.FileDescriptor),
	}
}

func (p *protoCache) Get(schemaId int) (*desc.FileDescriptor, bool) {
	fd, ok := p.m[schemaId]
	return fd, ok
}

func (p *protoCache) Put(schemaId int, fd *desc.FileDescriptor) {
	p.Lock()
	p.m[schemaId] = fd
	p.Unlock()
}
