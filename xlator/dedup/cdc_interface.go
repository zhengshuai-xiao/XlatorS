package dedup

import (
	"io"
	"strings"
)

// Chunker is an interface that returns the next chunk from a stream.
// The returned chunk's data is only valid until the next call to Next().
type Chunker interface {
	Next() (Chunk, error)
}

// CDC is an interface for creating chunkers from a reader.
type CDC interface {
	NewChunker(r io.Reader) (Chunker, error)
}

const (
	//why is this? because I do not want to change miniogo client, have to use this field
	userDefinedChunkMethodKey = "X-Amz-Meta-Chunk-Method"
)

func getCDCAlgorithm(x *XlatorDedup, userDefined map[string]string) CDC {
	// Default to FastCDC if no specific method is requested.
	chunkMethod := "FastCDC"
	if method, ok := userDefined[userDefinedChunkMethodKey]; ok {
		chunkMethod = method
	}

	switch strings.ToUpper(chunkMethod) {
	case "FIXEDCDC":
		logger.Tracef("Using FixedCDC chunking algorithm as requested by user tags.")
		return &FixedCDC{
			ChunkSize: x.fastCDCAvgSize, // Use average size as the fixed chunk size.
		}
	case "FASTCDC":
		logger.Tracef("Using FastCDC chunking algorithm.")
		return &FastCDC{
			MinChunkSize: x.fastCDCMinSize,
			AvgChunkSize: x.fastCDCAvgSize,
			MaxChunkSize: x.fastCDCMaxSize,
		}
	default:
		logger.Warnf("Unknown chunk_method '%s' requested, defaulting to FastCDC.", chunkMethod)
		return &FastCDC{
			MinChunkSize: x.fastCDCMinSize,
			AvgChunkSize: x.fastCDCAvgSize,
			MaxChunkSize: x.fastCDCMaxSize,
		}
	}
}
