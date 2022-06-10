package lib

import (
	"io"
	"strings"
	"testing"

	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/stretchr/testify/assert"
)

func TestResolveProtobuf(t *testing.T) {
	schema := `syntax = "proto3";
		package test.package;

		message MessageA {
			int32 FieldA = 1;
			message MessageB {
				message MessageC {
					int32 FieldABC = 1;
				}
			}
			message MessageD {
					int32 FieldAD = 1;
			}
			message MessageE {
				message MessageF {
					int32 FieldAEF = 1;
				}
				message MessageG {
					int32 FieldAEG = 1;
				}
				int32 FieldAE = 1;
			}
			int32 FieldA2 = 2;
		}
		message MessageH {
			message MessageI {
				int32 FieldHI = 1;
			}
		}`

	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(filename)), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(schema)
	assert.NoError(t, err)

	fileDescriptor := fileDescriptors[0]

	msg, err := resolveDescriptorByIndexes([]int{0, 2, 1}, fileDescriptor)
	assert.NoError(t, err)
	assert.Equal(t, "MessageG", msg.GetName())

	msg, err = resolveDescriptorByIndexes([]int{1, 0}, fileDescriptor)
	assert.NoError(t, err)
	assert.Equal(t, "MessageI", msg.GetName())
}
