package compression

type CompressionType byte

const (
	Compress_none   CompressionType = iota //0
	Compress_snappy                        //1
	Compress_zlib
)

var (
	CompressionMethods = map[string]CompressionType{
		"none":   Compress_none,
		"snappy": Compress_snappy,
		"zlib":   Compress_zlib,
	}
)

// Compressor defines the interface for data compression and decompression algorithms.
type Compressor interface {
	// Compress takes a byte slice and returns the compressed data.
	Compress(data []byte) ([]byte, error)

	// Decompress takes a compressed byte slice and returns the original data.
	Decompress(data []byte) ([]byte, error)

	// Type returns the type of compression, e.g., "zlib", "snappy".
	TypeString() string
	Type() CompressionType
}

func GetCompressorViaString(compressionStr string) (Compressor, error) {

	compressionType, ok := CompressionMethods[compressionStr]
	if !ok {
		return nil, ErrInvalidCompressionType
	}
	switch compressionType {
	case Compress_none:
		return nil, nil
	case Compress_zlib:
		return NewZlib(), nil
	case Compress_snappy:
		return NewSnappy(), nil
	default:
		return nil, ErrInvalidCompressionType
	}
}

func GetCompressorViaType(compressionType CompressionType) (Compressor, error) {
	switch compressionType {
	case Compress_none:
		return nil, nil
	case Compress_zlib:
		return NewZlib(), nil
	case Compress_snappy:
		return NewSnappy(), nil
	default:
		return nil, ErrInvalidCompressionType
	}
}
