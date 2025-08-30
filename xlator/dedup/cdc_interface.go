package dedup

import "io"

// Chunker is an interface that returns the next chunk from a stream.
// The returned chunk's data is only valid until the next call to Next().
type Chunker interface {
	Next() (Chunk, error)
}

// CDC is an interface for creating chunkers from a reader.
type CDC interface {
	NewChunker(r io.Reader) (Chunker, error)
}
