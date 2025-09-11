package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZlibCompressor(t *testing.T) {
	compressor := NewZlib()

	originalData := []byte("hello, world! this is a test string for zlib compression. it should be compressed and then decompressed back to the original string.")

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
	assert.Equal(t, "zlib", compressor.TypeString())
}

func TestZlibCompressor_EmptyData(t *testing.T) {
	compressor := NewZlib()

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

func TestZlibCompressor_DecompressInvalidData(t *testing.T) {
	compressor := NewZlib()

	invalidData := []byte("this is not valid zlib data")

	_, err := compressor.Decompress(invalidData)
	assert.Error(t, err, "Decompressing invalid data should return an error")
}
