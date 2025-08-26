package dedup

import (
	"errors"
)

// FastCDC implements the CDC interface using a simplified content-defined chunking approach.
// This implementation is for demonstration purposes and uses a basic rolling sum hash
// to identify chunk boundaries. It does not include the advanced optimizations or
// specific polynomial arithmetic found in a production-grade FastCDC algorithm.
type FastCDC struct {
	MinChunkSize uint32 // Minimum size of a chunk (e.g., 4KB)
	MaxChunkSize uint32 // Maximum size of a chunk (e.g., 64KB)
	WindowSize   uint32 // Size of the sliding window for rolling hash calculation (e.g., 64 bytes)
	BoundaryMask uint32 // Mask for boundary detection (e.g., 0x00000FFF for 12 zero bits)
}

// NewFastCDC creates a new FastCDC chunker with specified parameters.
// Recommended conceptual values: min=4096, max=65536, window=64, boundaryMask=0x00000FFF
func NewFastCDC(min, max, window, boundaryMask uint32) *FastCDC {
	return &FastCDC{
		MinChunkSize: min,
		MaxChunkSize: max,
		WindowSize:   window,
		BoundaryMask: boundaryMask,
	}
}

// Chunking implements the CDC interface for FastCDC.
// It takes a byte buffer and returns a slice of Chunk structs.
// This function uses a simple rolling sum hash for demonstration.
// A real FastCDC implementation would use a more robust rolling hash like Rabin fingerprint
// with specific polynomial properties for better distribution and collision resistance.
func (f *FastCDC) Chunking(buf []byte) ([]Chunk, error) {
	if len(buf) == 0 {
		return nil, nil
	}

	if f.MinChunkSize == 0 || f.MaxChunkSize == 0 || f.MinChunkSize > f.MaxChunkSize || f.WindowSize == 0 {
		return nil, errors.New("invalid FastCDC parameters: MinChunkSize, MaxChunkSize, WindowSize must be positive and MinChunkSize <= MaxChunkSize")
	}

	var chunks []Chunk
	dataLen := uint64(len(buf))
	currentOffset := uint64(0)

	// Initialize rolling sum for the first window
	var currentRollingSum uint32
	for i := uint32(0); i < f.WindowSize && uint64(i) < dataLen; i++ {
		currentRollingSum += uint32(buf[i])
	}

	// Iterate through the buffer to find chunk boundaries
	for i := uint64(f.WindowSize); i < dataLen; i++ {
		// Update rolling sum: subtract oldest byte, add new byte
		currentRollingSum -= uint32(buf[i-uint64(f.WindowSize)])
		currentRollingSum += uint32(buf[i])

		currentChunkLen := i - currentOffset + 1

		// Check for chunk boundary conditions:
		// 1. If current chunk length reaches MaxChunkSize, force a boundary.
		// 2. If current chunk length is at least MinChunkSize AND the rolling sum
		//    matches the boundary condition (e.g., ends with specific bits).
		if currentChunkLen >= uint64(f.MaxChunkSize) ||
			(currentChunkLen >= uint64(f.MinChunkSize) && (currentRollingSum&f.BoundaryMask) == 0) {

			chunks = append(chunks, Chunk{
				off:     currentOffset,
				Len:     currentChunkLen,
				Deduped: false,
				DOid:    0,
			})
			currentOffset = i + 1
		}
	}

	// Add any remaining data as the last chunk
	if currentOffset < dataLen {
		chunks = append(chunks, Chunk{
			off:     currentOffset,
			Len:     dataLen - currentOffset,
			Deduped: false,
			DOid:    0,
		})
	}

	return chunks, nil
}
