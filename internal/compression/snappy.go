package compression

import "github.com/golang/snappy"

// SnappyCompressor implements the Compressor interface using Snappy.
type SnappyCompressor struct{}

// NewSnappy returns a new SnappyCompressor.
func NewSnappy() *SnappyCompressor {
	return &SnappyCompressor{}
}

// Type returns the compression type.
func (c *SnappyCompressor) Type() CompressionType {
	return Compress_snappy
}

// Type returns the compression type string.
func (c *SnappyCompressor) TypeString() string {
	return "snappy"
}

// Compress compresses data using Snappy.
func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// Decompress decompresses data using Snappy.
func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}
	if decompressed == nil {
		return []byte{}, nil
	}
	return decompressed, nil
}
