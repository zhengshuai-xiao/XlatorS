package dedup

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalcFPs(t *testing.T) {
	chunks := []Chunk{
		{Data: []byte("hello"), Len: 5},
		{Data: []byte("world"), Len: 5},
		{Data: []byte(""), Len: 0},
	}

	// Calculate fingerprints
	CalcFPs(chunks)

	// Verify fingerprints
	h1 := sha256.Sum256([]byte("hello"))
	assert.Equal(t, string(h1[:]), chunks[0].FP)
	assert.Len(t, chunks[0].FP, 32, "Fingerprint should be 32 bytes (SHA256)")

	h2 := sha256.Sum256([]byte("world"))
	assert.Equal(t, string(h2[:]), chunks[1].FP)

	h3 := sha256.Sum256([]byte(""))
	assert.Equal(t, string(h3[:]), chunks[2].FP)
}
