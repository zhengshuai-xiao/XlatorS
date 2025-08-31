package dedup

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFixedCDC(t *testing.T) {
	testCases := []struct {
		name          string
		input         []byte
		chunkSize     int
		expectedLens  []int
		expectedError error
	}{
		{
			name:          "Empty Input",
			input:         []byte{},
			chunkSize:     10,
			expectedLens:  []int{},
			expectedError: io.EOF,
		},
		{
			name:          "Single Partial Chunk",
			input:         []byte("hello"),
			chunkSize:     10,
			expectedLens:  []int{5},
			expectedError: io.EOF,
		},
		{
			name:          "Single Full Chunk",
			input:         []byte("0123456789"),
			chunkSize:     10,
			expectedLens:  []int{10},
			expectedError: io.EOF,
		},
		{
			name:          "Multiple Full Chunks",
			input:         []byte("01234567890123456789"),
			chunkSize:     10,
			expectedLens:  []int{10, 10},
			expectedError: io.EOF,
		},
		{
			name:          "Multiple Chunks with Last Partial",
			input:         []byte("01234567890123456789abc"),
			chunkSize:     10,
			expectedLens:  []int{10, 10, 3},
			expectedError: io.EOF,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cdc := &FixedCDC{ChunkSize: tc.chunkSize}
			chunker, err := cdc.NewChunker(bytes.NewReader(tc.input))
			assert.NoError(t, err)

			var chunks []Chunk
			var finalErr error
			for {
				chunk, err := chunker.Next()
				if err != nil {
					finalErr = err
					break
				}
				chunks = append(chunks, chunk)
			}

			assert.Equal(t, tc.expectedError, finalErr)
			assert.Len(t, chunks, len(tc.expectedLens))
			for i, chunk := range chunks {
				assert.Equal(t, tc.expectedLens[i], int(chunk.Len))
				assert.Len(t, chunk.Data, tc.expectedLens[i])
			}
		})
	}
}
