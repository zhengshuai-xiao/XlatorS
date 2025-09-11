package internal

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateCRC32(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"Simple String", []byte("hello, world")},
		{"Empty Slice", []byte{}},
		{"Binary Data", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedCRC := crc32.ChecksumIEEE(tc.data)
			calculatedCRC := CalculateCRC32(tc.data)
			assert.Equal(t, expectedCRC, calculatedCRC)
		})
	}
}
