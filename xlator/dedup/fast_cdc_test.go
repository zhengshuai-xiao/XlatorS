package dedup

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastCDC(t *testing.T) {
	// Generate some test data that is likely to produce variable-sized chunks.
	// A mix of repeating and random data is good for this.
	var data []byte
	for i := 0; i < 100; i++ {
		data = append(data, bytes.Repeat([]byte{byte(i)}, 100)...)
	}
	for i := 0; i < 1024; i++ {
		data = append(data, 'a')
	}

	minSize := 1024
	avgSize := 4096
	maxSize := 8192

	cdc := &FastCDC{
		MinChunkSize: minSize,
		AvgChunkSize: avgSize,
		MaxChunkSize: maxSize,
	}

	chunker, err := cdc.NewChunker(bytes.NewReader(data))
	assert.NoError(t, err)

	var chunks []Chunk
	var totalSize int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		chunks = append(chunks, chunk)

		// Assert that each chunk respects the size boundaries.
		assert.GreaterOrEqual(t, int(chunk.Len), minSize)
		assert.LessOrEqual(t, int(chunk.Len), maxSize)
		totalSize += int(chunk.Len)
	}

	assert.Equal(t, len(data), totalSize, "Total size of chunks should equal input size")
	assert.Greater(t, len(chunks), 0, "Should produce at least one chunk")
}
