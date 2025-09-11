package compression

import (
	"bytes"
	"compress/zlib"
	"io"
)

// ZlibCompressor implements the Compressor interface using Zlib.
type ZlibCompressor struct{}

// NewZlib returns a new ZlibCompressor.
func NewZlib() *ZlibCompressor {
	return &ZlibCompressor{}
}

func (c *ZlibCompressor) Type() CompressionType {
	return Compress_zlib
}

// Type returns the compression type.
func (c *ZlibCompressor) TypeString() string {
	return "zlib"
}

// Compress compresses data using Zlib.
func (c *ZlibCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Decompress decompresses data using Zlib.
func (c *ZlibCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
