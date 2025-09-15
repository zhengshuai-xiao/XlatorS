package dedup

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// setupTestXlator creates a new XlatorDedup instance with a mock MDS client
// and a temporary cache directory for testing purposes. It returns the xlator,
// the mock client, and a teardown function to clean up the cache directory.
func setupTestXlator(t *testing.T) (*XlatorDedup, *MockMDS, func()) {
	cacheDir, err := os.MkdirTemp("", "dedup-test-cache-")
	assert.NoError(t, err)

	mockMDS := new(MockMDS)

	xlator := &XlatorDedup{
		Mdsclient:     mockMDS,
		dcCachePath:   cacheDir,
		dsBackendType: DObjBackendPOSIX, // Use POSIX for most tests to avoid S3 dependency
		dcBackend:     &POSIXBackend{dcCachePath: cacheDir},
	}

	teardown := func() {
		os.RemoveAll(cacheDir)
	}

	return xlator, mockMDS, teardown
}

// padFP pads or truncates a string to 32 bytes for testing.
func padFP(s string) string {
	b := make([]byte, 32)
	copy(b, s)
	return string(b)
}
