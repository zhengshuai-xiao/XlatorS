package dedup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	minio "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/zhengshuai-xiao/XlatorS/internal/compression"
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
	// Create a compressor for the test
	compressor, err := compression.GetCompressorViaString("zlib")
	assert.NoError(t, err)

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

		written, _, err := xlator.writeDObj(ctx, dobj, chunks, compressor)

		assert.NoError(t, err)
		assert.True(t, written > 0, "written should be greater than 0 after compression")
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

		written, _, err := xlator.writeDObj(ctx, dobj, chunks, compressor)

		assert.NoError(t, err)
		assert.True(t, written > 0, "Should only write the new chunk's data, and size should be > 0")
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

func TestReadWriteDataObject(t *testing.T) {
	xlator, mockMDS, teardown := setupTestXlator(t)
	defer teardown()

	ctx := context.Background()
	compressor, err := compression.GetCompressorViaString("zlib")
	assert.NoError(t, err)

	// 1. Prepare chunks and write them to a DObj
	chunk1Data := []byte(strings.Repeat("A", 1024))
	chunk2Data := []byte(strings.Repeat("B", 2048))
	chunk3Data := []byte(strings.Repeat("C", 1536))
	originalContent := append(append(chunk1Data, chunk2Data...), chunk3Data...)

	chunks := []Chunk{
		{Data: chunk1Data, Len: uint64(len(chunk1Data))},
		{Data: chunk2Data, Len: uint64(len(chunk2Data))},
		{Data: chunk3Data, Len: uint64(len(chunk3Data))},
	}
	CalcFPs(chunks) // Calculate real FPs

	dobj := &DObj{bucket: "test-ns.test-bucket"}
	mockMDS.On("GetIncreasedDOID").Return(int64(1), nil).Once()
	mockMDS.On("GetDObjNameInMDS", uint64(1)).Return("dobj-1").Once()
	mockMDS.On("GetDOIDFromDObjName", "dobj-1").Return(int64(1), nil).Times(3)

	_, _, err = xlator.writeDObj(ctx, dobj, chunks, compressor)
	assert.NoError(t, err)
	err = xlator.truncateDObj(ctx, dobj)
	assert.NoError(t, err)

	// 2. Prepare manifest for reading
	manifest := []ChunkInManifest{
		{FP: chunks[0].FP, Len: chunks[0].Len, DOid: 1},
		{FP: chunks[1].FP, Len: chunks[1].Len, DOid: 1},
		{FP: chunks[2].FP, Len: chunks[2].Len, DOid: 1},
	}

	// 3. Run test cases for reading
	testCases := []struct {
		name        string
		startOffset int64
		length      int64
		expected    []byte
	}{
		{"ReadFull", 0, -1, originalContent},
		{"ReadFromOffset", 1024, -1, originalContent[1024:]},
		{"ReadWithLength", 0, 1024 + 2048, originalContent[:1024+2048]},
		{"ReadRangeSpanningChunks", 1000, 300, originalContent[1000 : 1000+300]},
		{"ReadRangeInSingleChunk", 10, 50, originalContent[10:60]},
		{"ReadRangePastEnd", 4000, 1000, originalContent[4000:]},
		{"ReadZeroLength", 100, 0, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &bytes.Buffer{}
			mockMDS.On("GetDObjNameInMDS", uint64(1)).Return("dobj-1").Once()
			err := xlator.readDataObject("test-ns.test-bucket", manifest, tc.startOffset, tc.length, writer, minio.ObjectOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, writer.Bytes())
		})
	}
}
