package lib

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var ErrTypeUndefinedInSchemaRegistry = errors.New("type is not defined in schema registry")
var ErrUnableToDecodeValueInMessageIndexArray = errors.New("unable to decode value in message index array")
var ErrUnableToDecodeMessageIndexArray = errors.New("unable to decode message index array")
var ErrInvalidProtobufWireProtocolVersion = errors.New("invalid protobuf wire protocol")
var ErrMissingMessageTypeInSchema = errors.New("unable to find MessageType for messageIndex inside schema")
var ErrNothingToDeserialize = errors.New("unable to deserialize nil")

// ProtobufResolver is an interface which can resolve a protobuf
// MessageDescriptor from topic name and the info contained in the
// message header and instantiate an instance of the message described
// by the MessageDescriptor
type ProtobufResolver interface {
	ResolveProtobuf(ctx context.Context, schemaId int, msgIndexes []int) (proto.Message, error)
}

// ProtobufRegistry is the minimum interface of protoregistry.Types registry
// needed to resolve MessageType from topic name (plus a registration function,
// for convenience)
type ProtobufRegistry interface {
	RangeMessages(f func(protoreflect.MessageType) bool)
	FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error)
}

// DeserializationType is a type alias for representing Key and Value
// deserialization types
type DeserializationType int

const (
	KeyDeserialization DeserializationType = iota
	ValueDeserialization
)

// SchemaRegistryProtobufResolver
type SchemaRegistryProtobufResolver struct {
	schemaRegistry      iSchemaRegistryClient
	protobufRegistry    ProtobufRegistry
	deserializationType DeserializationType
}

// NewSchemaRegistryProtobufResolver
func NewSchemaRegistryProtobufResolver(
	schemaRegistry iSchemaRegistryClient,
	protobufRegistry ProtobufRegistry,
	deserializationType DeserializationType,
) *SchemaRegistryProtobufResolver {
	return &SchemaRegistryProtobufResolver{
		schemaRegistry:      schemaRegistry,
		protobufRegistry:    protobufRegistry,
		deserializationType: deserializationType,
	}
}

// ResolveProtobuf
func (reg *SchemaRegistryProtobufResolver) ResolveProtobuf(
	ctx context.Context,
	schemaId int,
	msgIndexes []int,
) (proto.Message, error) {

	fileDescriptor, err := reg.schemaRegistry.GetProtoSchema(ctx, schemaId)
	if err != nil {
		return nil, err
	}

	msg, err := resolveDescriptorByIndexes(msgIndexes, fileDescriptor)
	if err != nil {
		return nil, err
	}

	var mt protoreflect.MessageType
	reg.protobufRegistry.RangeMessages(func(messageType protoreflect.MessageType) bool {
		if string(messageType.Descriptor().Name()) == msg.GetName() {
			mt = messageType
			return false
		}
		return true
	})
	if mt != nil {
		pb := mt.New()
		return pb.Interface(), nil
	}
	return nil, ErrMissingMessageTypeInSchema
}

// DeserializationFunc is a type that describes the function that is ultimately used to
// deserialize a protobuf.
type DeserializationFunc = func([]byte, proto.Message) error

// ProtobufDeserializer hydrates a []byte into a Protobuf which is resolved via
// a ProtobufResolver
type ProtobufDeserializer struct {
	protobufResolver ProtobufResolver
	unmarshal        DeserializationFunc
}

// VTUnmarshal is an inerface satisfied by any protobuf that has been built with
// the protoc-gen-go-vtproto tool to generate an efficient unmarshal method
type VTUnmarshal interface {
	UnmarshalVT(data []byte) error
}

// Unmarshal is a wrapper around proto.Unmarshal which will use UnmarshalVT when
// deserializing any proto that has been modified by protoc-gen-go-vtproto with
// the unmarshal option
func Unmarshal(bytes []byte, msg proto.Message) error {
	switch m := msg.(type) {
	case VTUnmarshal:
		return m.UnmarshalVT(bytes)
	default:
		return proto.Unmarshal(bytes, msg)
	}
}

// NewProtobufDeserializer is a constructor that takes a iSchemaRegistryClient
// and a ProtobufResolver, which are used to determine schema and resolve an
// empty protobuf that data can be unmarshalled into.
func NewProtobufDeserializer(
	protobufResolver ProtobufResolver,
	deserializationFunc ...DeserializationFunc,
) *ProtobufDeserializer {
	// marshall via Marshal by default
	unmarshal := Unmarshal
	if len(deserializationFunc) > 0 {
		unmarshal = deserializationFunc[0]
	}

	return &ProtobufDeserializer{
		protobufResolver: protobufResolver,
		unmarshal:        unmarshal,
	}
}

// Deserialize hydrates an []byte into a protobuf instance which is resolved
// from the topic name and schemaId by the ProtobufResolver
func (ps *ProtobufDeserializer) Deserialize(
	ctx context.Context,
	bytes []byte,
) (proto.Message, error) {
	if bytes == nil {
		return nil, ErrNothingToDeserialize
	}

	bytesRead, schemaId, msgIndexes, err := decodeHeader(bytes)

	// resolve an empty instance of correct protobuf
	pb, err := ps.protobufResolver.ResolveProtobuf(ctx, schemaId, msgIndexes)
	if err != nil {
		return nil, err
	}

	// unmarshal into the empty protobuf after the header in bytes
	err = ps.unmarshal(bytes[bytesRead:], pb)
	if err != nil {
		return nil, err
	}
	return pb, nil
}

func decodeHeader(
	bytes []byte,
) (totalBytesRead int, schemaId int, msgIndexes []int, err error) {
	if bytes[0] != byte(0) {
		err = ErrInvalidProtobufWireProtocolVersion
		return
	}
	// we should actually validate the schemaId against the topic in some way, but note that it
	// only needs to be compatible with the latest schema, not equal to it.
	schemaId = int(binary.BigEndian.Uint32(bytes[1:5]))

	// decode the number of elements in the array of message indexes
	arrayLen, bytesRead := binary.Varint(bytes[5:])
	if bytesRead <= 0 {
		err = ErrUnableToDecodeMessageIndexArray
		return
	}
	if arrayLen == 0 {
		msgIndexes = append(msgIndexes, 0)
	}

	totalBytesRead = 5 + bytesRead
	msgIndexes = make([]int, arrayLen)
	// iterate arrayLen times, decoding another varint
	for i := 0; i < int(arrayLen); i++ {
		idx, bytesRead := binary.Varint(bytes[totalBytesRead:])
		if bytesRead <= 0 {
			err = ErrUnableToDecodeValueInMessageIndexArray
			return
		}
		totalBytesRead += bytesRead
		msgIndexes[i] = int(idx)
	}
	return
}

func resolveDescriptorByIndexes(msgIndexes []int, descriptor desc.Descriptor) (desc.Descriptor, error) {
	if len(msgIndexes) == 0 {
		return descriptor, nil
	}

	index := msgIndexes[0]
	msgIndexes = msgIndexes[1:]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		if index >= len(v.GetMessageTypes()) {
			return nil, ErrTypeUndefinedInSchemaRegistry
		}
		return resolveDescriptorByIndexes(msgIndexes, v.GetMessageTypes()[index])
	case *desc.MessageDescriptor:
		if len(msgIndexes) > 0 {
			return resolveDescriptorByIndexes(msgIndexes, v.GetNestedMessageTypes()[index])
		} else {
			return v.GetNestedMessageTypes()[index], nil
		}
	default:
		return nil, ErrTypeUndefinedInSchemaRegistry
	}
}
