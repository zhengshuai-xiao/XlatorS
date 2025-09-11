package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnappyCompressor(t *testing.T) {
	compressor := NewSnappy()

	originalData := []byte("hello, world! this is a test string for snappy compression. it should be compressed and then decompressed back to the original string.")

	// Test compression
	compressedData, err := compressor.Compress(originalData)
	assert.NoError(t, err)
	assert.NotNil(t, compressedData)
	assert.Less(t, len(compressedData), len(originalData), "Compressed data should be smaller than original data")

	// Test decompression
	decompressedData, err := compressor.Decompress(compressedData)
	assert.NoError(t, err)
	assert.Equal(t, originalData, decompressedData, "Decompressed data should match original data")

	// Test Type
	assert.Equal(t, "snappy", compressor.TypeString())
}

func TestSnappyCompressor_EmptyData(t *testing.T) {
	compressor := NewSnappy()

	originalData := []byte{}

	// Test compression
	compressedData, err := compressor.Compress(originalData)
	assert.NoError(t, err)
	assert.NotNil(t, compressedData)

	// Test decompression
	decompressedData, err := compressor.Decompress(compressedData)
	assert.NoError(t, err)
	assert.Equal(t, originalData, decompressedData)
}

func TestSnappyCompressor_DecompressInvalidData(t *testing.T) {
	compressor := NewSnappy()

	invalidData := []byte("this is not valid snappy data")

	_, err := compressor.Decompress(invalidData)
	assert.Error(t, err, "Decompressing invalid data should return an error")
}
