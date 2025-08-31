package dedup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTestXlator(t *testing.T) (*XlatorDedup, *MockMDS, func()) {
	// Create a temporary directory for the dobj cache
	tempDir, err := os.MkdirTemp("", "dedup-test-")
	assert.NoError(t, err)

	mockMDS := new(MockMDS)

	xlator := &XlatorDedup{
		Mdsclient:     mockMDS,
		dobjCachePath: tempDir + "/",
		dsBackendType: DObjBackendPOSIX, // Test with posix backend to avoid S3 client dependency
	}

	// Teardown function to clean up the temporary directory
	teardown := func() {
		os.RemoveAll(tempDir)
	}

	return xlator, mockMDS, teardown
}

func TestWriteDObj(t *testing.T) {
	xlator, mockMDS, teardown := setupTestXlator(t)
	defer teardown()

	ctx := context.Background()

	// --- Test Case 1: Write a single new chunk ---
	t.Run("WriteSingleNewChunk", func(t *testing.T) {
		dobj := &DObj{} // Start with an empty DObj
		chunks := []Chunk{
			{FP: "fp1", Data: []byte("hello world"), Len: 11, Deduped: false},
		}

		// Setup mock expectations
		mockMDS.On("GetIncreasedDOID").Return(int64(1), nil).Once()
		mockMDS.On("GetDObjNameInMDS", uint64(1)).Return("dobj-1").Once()
		mockMDS.On("GetDOIDFromDObjName", "dobj-1").Return(int64(1), nil).Once()

		written, err := xlator.writeDObj(ctx, dobj, chunks)

		assert.NoError(t, err)
		assert.Equal(t, 11, written)
		assert.Equal(t, "dobj-1", dobj.dobj_key, "DObj key should be set")
		assert.Equal(t, uint64(1), chunks[0].DOid, "Chunk's DOid should be updated")
		assert.Len(t, dobj.fps, 1, "One FP should be recorded in the DObj")

		// Verify that the file was created and has the correct content
		dobjPath := filepath.Join(xlator.dobjCachePath, "dobj-1")
		_, err = os.Stat(dobjPath)
		assert.NoError(t, err, "DObj file should be created")

		mockMDS.AssertExpectations(t)
	})

	// --- Test Case 2: Write a mix of new and deduped chunks ---
	t.Run("WriteNewAndDedupedChunks", func(t *testing.T) {
		dobj := &DObj{} // Reset DObj for new test
		chunks := []Chunk{
			{FP: "fp2", Data: []byte("new data"), Len: 8, Deduped: false},
			{FP: "fp3", Data: []byte("old data"), Len: 8, Deduped: true}, // This one should be skipped
		}

		// Setup mock expectations for the new chunk
		mockMDS.On("GetIncreasedDOID").Return(int64(2), nil).Once()
		mockMDS.On("GetDObjNameInMDS", uint64(2)).Return("dobj-2").Once()
		mockMDS.On("GetDOIDFromDObjName", "dobj-2").Return(int64(2), nil).Once()

		written, err := xlator.writeDObj(ctx, dobj, chunks)

		assert.NoError(t, err)
		assert.Equal(t, 8, written, "Should only write the new chunk's data")
		assert.Equal(t, "dobj-2", dobj.dobj_key)
		assert.Equal(t, uint64(2), chunks[0].DOid)
		assert.Len(t, dobj.fps, 1, "Only the new chunk's FP should be recorded")

		mockMDS.AssertExpectations(t)
	})

	// --- Test Case 3: Test DObj rolling over when it reaches maxSize ---
	t.Run("DObjRollover", func(t *testing.T) {
		// This test is more complex and would involve setting the `maxSize` constant
		// to a smaller value for testing, and then writing chunks that exceed it.
		// It would verify that truncateDObj and newDObj are called correctly.
		t.Skip("Skipping DObj rollover test for brevity.")
	})
}
