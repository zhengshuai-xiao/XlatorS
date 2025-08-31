package fastcdc

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFastCDCChunker_Validation tests the validation logic of NewChunker.
func TestFastCDCChunker_Validation(t *testing.T) {
	testCases := []struct {
		name        string
		opts        Options
		expectError bool
	}{
		{
			name:        "Valid Options",
			opts:        Options{AverageSize: 4096, MinSize: 1024, MaxSize: 8192},
			expectError: false,
		},
		{
			name:        "Missing AverageSize",
			opts:        Options{MinSize: 1024, MaxSize: 8192},
			expectError: true,
		},
		{
			name:        "MinSize too small",
			opts:        Options{AverageSize: 4096, MinSize: 10, MaxSize: 8192},
			expectError: true,
		},
		{
			name:        "MaxSize too large",
			opts:        Options{AverageSize: 4096, MinSize: 1024, MaxSize: 1 << 31},
			expectError: true,
		},
		{
			name:        "MinSize >= MaxSize",
			opts:        Options{AverageSize: 4096, MinSize: 8192, MaxSize: 4096},
			expectError: true,
		},
		{
			name:        "AverageSize not between Min and Max",
			opts:        Options{AverageSize: 10000, MinSize: 1024, MaxSize: 8192},
			expectError: true,
		},
		{
			name:        "BufSize < MaxSize",
			opts:        Options{AverageSize: 4096, MinSize: 1024, MaxSize: 8192, BufSize: 4096},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewChunker(bytes.NewReader([]byte{}), tc.opts)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFastCDCChunker_ChunkingLogic tests the core chunking logic.
func TestFastCDCChunker_ChunkingLogic(t *testing.T) {
	// A mix of repeating and somewhat random data
	data := func() []byte {
		d := make([]byte, 0, 20000)
		d = append(d, bytes.Repeat([]byte{0x01}, 5000)...)
		d = append(d, bytes.Repeat([]byte{0xFF}, 5000)...)
		for i := 0; i < 10000; i++ {
			d = append(d, byte(i))
		}
		return d
	}()

	opts := Options{AverageSize: 4096, MinSize: 1024, MaxSize: 8192}

	chunker, err := NewChunker(bytes.NewReader(data), opts)
	assert.NoError(t, err)

	var chunks []Chunk
	var totalSize int
	var lastOffset int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		chunks = append(chunks, chunk)

		// Assert that each chunk respects the size boundaries (unless it's the very last one and smaller than minSize)
		if len(chunk.Data) < len(data) { // If not the only chunk
			assert.GreaterOrEqual(t, chunk.Length, opts.MinSize, "Chunk length should be >= MinSize")
		}
		assert.LessOrEqual(t, chunk.Length, opts.MaxSize, "Chunk length should be <= MaxSize")

		// Assert data integrity
		assert.Equal(t, data[chunk.Offset:chunk.Offset+chunk.Length], chunk.Data)

		// Assert offset is correct
		assert.Equal(t, lastOffset, chunk.Offset)
		lastOffset += chunk.Length

		totalSize += chunk.Length
	}

	assert.Equal(t, len(data), totalSize, "Total size of chunks should equal input size")
	assert.Greater(t, len(chunks), 1, "Should produce multiple chunks for this test case")
}
