# Protobuf Usage Examples

If you plan to use srclient with Protobuf schemas, please do give the issues #16 and #17 a read for a better understanding on how to do so.

* [Issue #16](https://github.com/riferrei/srclient/issues/16)
* [Issue #17](https://github.com/riferrei/srclient/issues/17)

## Example In #16

### Producer

```go

func makeNotifEventMessage() []byte {
	event := &metrics.Event{       # My Protobuf message
		Desc:         "Email got sent successfully",
		SourceSystem: "test-producer",
		Ts:           timestamppb.Now(),
		Type:         metrics.Event_EMAIL_SUCCESS,
		},
	}
	metricAny, _ := anypb.New(event)    # This wraps the "event"  protobuf object in an "Any" protobuf type. 

	out, _ := proto.Marshal(metricAny)
	return out
}
```

### Consumer

```go

# Handle getting Kafka messages

# Unmarshal in to an "Any" type, which is wrapping the "real" message.
wrapper := &anypb.Any{} 
if err := proto.Unmarshal(kafkaMessage.Value, wrapper); err != nil {
	c.log.Error("Failed to parse event", zap.Error(err))
	continue
}

// This is probably where you could involve protoregistry in some fancy way
// and anypb.UnmarshalNew(), but for now we can keep it simple.

var msgBytes []byte

// Iterate over the possible message types and process them.
switch wrapper.MessageName() {   # The "Any" type has a MessageName field which allows you to see what kind of protobuf message the wrapped object is.
case "metrics.Event":
	me := &metrics.Event{}

	anypb.UnmarshalTo(wrapper, me, proto.UnmarshalOptions{})
	msgBytes = processMetricsEvent(me)
default:
	c.log.Info(fmt.Sprintf("Unknown message: %s, skipping.", wrapper.MessageName()))
	c.commitMessages(ctx, []kafka.Message{})
	continue
}
```

## Example in #17

### Protobuf Resolver
```go
// SchemaRegistryProtobufResolver
type SchemaRegistryProtobufResolver struct {
	schemaRegistry      SchemaRegistryClient
	protobufRegistry    ProtobufRegistry
	deserializationType DeserializationType
}

// NewSchemaRegistryProtobufResolver
func NewSchemaRegistryProtobufResolver(
	schemaRegistry SchemaRegistryClient,
	protobufRegistry ProtobufRegistry,
	deserializationType DeserializationType,
) *SchemaRegistryProtobufResolver {
	return &SchemaRegistryProtobufResolver{
		schemaRegistry:      schemaRegistry,
		protobufRegistry:    protobufRegistry,
		deserializationType: deserializationType,
	}
}

// This should probably exist in srclient
func (reg *SchemaRegistryProtobufResolver) parseSchema(schemaId int) (*desc.FileDescriptor, error) {
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema *srclient.Schema
			var err error

			// filename is a schema id, fetch it directly
			if schemaId, err = strconv.Atoi(filename); err == nil {
				schema, err = reg.schemaRegistry.GetSchema(schemaId)
			} else {
				// otherwise its likely an import and we look it up by its filename
				schema, err = reg.schemaRegistry.GetLatestSchema(filename)
			}

			if err != nil {
				return nil, err
			}
			if *(schema.SchemaType()) != srclient.Protobuf {
				return nil, fmt.Errorf("schema %v is not a Protobuf schema", schemaId)
			}
			return io.NopCloser(strings.NewReader(schema.Schema())), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(strconv.Itoa(schemaId))
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("unexpected schema from schema registry")
	}
	return fileDescriptors[0], nil
}

// ResolveProtobuf
func (reg *SchemaRegistryProtobufResolver) ResolveProtobuf(
	schemaId int,
	msgIndexes []int,
) (proto.Message, error) {

	fileDescriptor, err := reg.parseSchema(schemaId)
	if err != nil {
		return nil, err
	}

	msg := resolveDescriptorByIndexes(msgIndexes, fileDescriptor)

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
	return nil, fmt.Errorf("unable to find MessageType for messageIndex %v inside schema %v", msgIndexes, schemaId)
}

func resolveDescriptorByIndexes(msgIndexes []int, descriptor desc.Descriptor) desc.Descriptor {
	if len(msgIndexes) == 0 {
		return descriptor
	}

	index := msgIndexes[0]
	msgIndexes = msgIndexes[1:]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		return resolveDescriptorByIndexes(msgIndexes, v.GetMessageTypes()[index])
	case *desc.MessageDescriptor:
		if len(msgIndexes) > 0 {
			return resolveDescriptorByIndexes(msgIndexes, v.GetNestedMessageTypes()[index])
		} else {
			return v.GetNestedMessageTypes()[index]
		}
	default:
		fmt.Printf("no match: %v\n", v)
		return nil
	}
}
```

### Example Usage

```go
schemaRegistryClient := srclient.CreateSchemaRegistryClient(lib.SchemaRegistryUrl)
	schemaRegistryClient.SetCredentials(lib.SchemaRegistryUsername, lib.SchemaRegistryPassword)
	protobufResolver := lib.NewSchemaRegistryProtobufResolver(schemaRegistryClient, protoregistry.GlobalTypes, lib.ValueDeserialization)
	deserializer := lib.NewProtobufDeserializer(protobufResolver)

	for {
		msg, err := c.ReadMessage(60 * time.Second)
		if err == nil {
			value, err := deserializer.Deserialize(msg.Value)
			if err != nil {
				sugar.Fatal(err)
			}

			switch v := value.(type) {
			case *schema.SampleRecord:
				sugar.Infof("Here is the sample record: (%s), headers (%v)", v.String(), msg.Headers)
			case *schema.OtherRecord_NestedRecord:
				sugar.Infof("Here is the nested record: (%s), headers (%v)", v.String(), msg.Headers)
			case *schema.OtherRecord:
				sugar.Infof("Here is the other record: (%s), headers (%v)", v.String(), msg.Headers)
			default:
				sugar.Infof("unrecognized message type: %T", v)
			}
		} else {
			sugar.Infof("Error consuming the message: %v (%v)", err, msg)
		}
	}
```

### Protobuf Schema 

```protobuf
syntax = "proto3";
package com.mycorp.mynamespace;

message SampleRecord {
  int32 my_field1 = 1;
  double my_field2 = 2;
  string my_field3 = 3;
  string my_field4 = 4;
}
message OtherRecord {
  string field = 1;

  message NestedRecord {
    string nestedfield = 1;
  }
}
 ```