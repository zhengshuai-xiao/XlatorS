package dedup

import (
	"io"

	fastcdc "github.com/zhengshuai-xiao/XlatorS/pkg/cdc"
)

// FastCDC implements the CDC interface to create FastCDC chunkers.
type FastCDC struct {
	MinChunkSize int
	AvgChunkSize int
	MaxChunkSize int
}

// NewChunker creates a new chunker that reads from r and produces variable-size chunks using FastCDC.
func (f *FastCDC) NewChunker(r io.Reader) (Chunker, error) {
	opts := fastcdc.Options{
		MinSize:     f.MinChunkSize,
		AverageSize: f.AvgChunkSize,
		MaxSize:     f.MaxChunkSize,
	}
	chunker, err := fastcdc.NewChunker(r, opts)
	if err != nil {
		return nil, err
	}
	return &fastCDCChunker{
		chunker: chunker,
	}, nil
}

// fastCDCChunker implements the Chunker interface for FastCDC.
type fastCDCChunker struct {
	chunker *fastcdc.Chunker
}

// Next returns the next content-defined chunk from the reader.
func (c *fastCDCChunker) Next() (Chunk, error) {
	fc, err := c.chunker.Next()
	if err != nil {
		return Chunk{}, err // Propagate io.EOF and other errors
	}

	// The data slice from fastcdc is a view into its internal buffer.
	// We must copy it to a new slice to ensure its lifetime outside the chunker.
	dataCopy := make([]byte, fc.Length)
	copy(dataCopy, fc.Data)

	return Chunk{
		Data:    dataCopy,
		Len:     uint64(fc.Length),
		Deduped: false,
	}, nil
}
