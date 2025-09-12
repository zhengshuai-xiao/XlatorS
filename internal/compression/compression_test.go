package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCompressor(t *testing.T) {
	t.Run("GetCompressorViaString", func(t *testing.T) {
		// Test valid types
		c, err := GetCompressorViaString("zlib")
		assert.NoError(t, err)
		assert.IsType(t, &ZlibCompressor{}, c)

		c, err = GetCompressorViaString("snappy")
		assert.NoError(t, err)
		assert.IsType(t, &SnappyCompressor{}, c)

		c, err = GetCompressorViaString("none")
		assert.NoError(t, err)
		assert.Nil(t, c)

		// Test invalid type
		c, err = GetCompressorViaString("invalid")
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidCompressionType, err)
		assert.Nil(t, c)
	})

	t.Run("GetCompressorViaType", func(t *testing.T) {
		// Test valid types
		c, err := GetCompressorViaType(Compress_zlib)
		assert.NoError(t, err)
		assert.IsType(t, &ZlibCompressor{}, c)

		c, err = GetCompressorViaType(Compress_snappy)
		assert.NoError(t, err)
		assert.IsType(t, &SnappyCompressor{}, c)

		c, err = GetCompressorViaType(Compress_none)
		assert.NoError(t, err)
		assert.Nil(t, c)

		// Test invalid type
		c, err = GetCompressorViaType(99) // Some invalid type
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidCompressionType, err)
		assert.Nil(t, c)
	})
}
