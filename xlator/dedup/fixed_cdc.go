package dedup

import "io"

// FixedCDC implements the CDC interface to create fixed-size chunkers.
type FixedCDC struct {
	ChunkSize int
}

// NewChunker creates a new chunker that reads from r and produces fixed-size chunks.
func (f *FixedCDC) NewChunker(r io.Reader) (Chunker, error) {
	return &fixedChunker{
		r:         r,
		chunkSize: f.ChunkSize,
	}, nil
}

// fixedChunker implements the Chunker interface for fixed-size chunking.
type fixedChunker struct {
	r         io.Reader
	chunkSize int
}

// Next returns the next fixed-size chunk from the reader.
func (c *fixedChunker) Next() (Chunk, error) {
	buf := make([]byte, c.chunkSize)
	n, err := io.ReadFull(c.r, buf)

	if err == io.EOF { // Clean end of stream, no bytes read.
		return Chunk{}, io.EOF
	}
	if err == io.ErrUnexpectedEOF { // Last partial chunk.
		return Chunk{
			Data:    buf[:n],
			Len:     uint64(n),
			Deduped: false,
		}, nil
	}
	if err != nil { // Some other error occurred.
		return Chunk{}, err
	}

	// A full chunk was read.
	return Chunk{
		Data:    buf,
		Len:     uint64(c.chunkSize),
		Deduped: false,
	}, nil
}
