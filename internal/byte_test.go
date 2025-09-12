package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHexConversion(t *testing.T) {
	testCases := []struct {
		name     string
		original string
		hex      string
	}{
		{
			name:     "Simple String",
			original: "hello",
			hex:      "68656c6c6f",
		},
		{
			name:     "String with Numbers",
			original: "12345",
			hex:      "3132333435",
		},
		{
			name:     "Empty String",
			original: "",
			hex:      "",
		},
		{
			name:     "String with Non-printable chars",
			original: string([]byte{0x00, 0x01, 0xDE, 0xAD, 0xBE, 0xEF}),
			hex:      "0001deadbeef",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test StringToHex
			assert.Equal(t, tc.hex, StringToHex(tc.original))

			// Test HexToString
			converted, err := HexToString(tc.hex)
			assert.NoError(t, err)
			assert.Equal(t, tc.original, converted)
		})
	}
}

func TestUInt64Conversion(t *testing.T) {
	original := uint64(0x0102030405060708)
	bytes := UInt64ToBytesLittleEndian(original)
	assert.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, bytes[:])

	converted := BytesToUInt64LittleEndian(bytes)
	assert.Equal(t, original, converted)
}

type serializableStruct struct {
	Message string
	Value   int
}

func TestStringSerialization(t *testing.T) {
	original := serializableStruct{Message: "hello", Value: 42}

	// Test SerializeToString
	serialized, err := SerializeToString(original)
	assert.NoError(t, err)
	assert.NotEmpty(t, serialized)

	// Test DeserializeFromString
	var deserialized serializableStruct
	err = DeserializeFromString(serialized, &deserialized)
	assert.NoError(t, err)

	assert.Equal(t, original, deserialized)
}
