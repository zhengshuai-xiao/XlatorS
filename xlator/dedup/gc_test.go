package dedup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	minio "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/assert"
)

func TestCleanupNamespace(t *testing.T) {
	xlator, mockMDS, teardown := setupTestXlator(t)
	defer teardown()

	ctx := context.Background()
	namespace := "test-ns"

	// --- Test Case: Successfully clean up a single DOID ---
	t.Run("CleanupSingleDOID", func(t *testing.T) {
		doid := uint64(100)
		dobjName := "dobj-100"
		dobjPath := filepath.Join(xlator.dobjCachePath, dobjName)

		// Create a dummy DObj file to be deleted
		f, err := os.Create(dobjPath)
		assert.NoError(t, err)
		f.Close()

		// Mock the DObj content (for fpmap)
		dummyFP := "dummy-fingerprint-string-for-gc"
		fpMap := make(map[string]fpinDObj)
		fpMap[dummyFP] = fpinDObj{Offset: 8, Len: 10}

		// Setup mock expectations
		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{doid}, nil).Once()
		mockMDS.On("GetDObjNameInMDS", doid).Return(dobjName)

		mockMDS.On("RemoveFPs", namespace, []string{dummyFP}, doid).Return(nil).Once()

		// Mock the final removal of the DOID from the deleted set
		mockMDS.On("RemoveSpecificDeletedDOIDs", namespace, []uint64{doid}).Return(nil).Once()

		// Mock the end of the GC run for this namespace
		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		// Create a mock getDataObject function for this test case.
		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DObjReader, error) {
			// Return a mocked DObjReader
			file, _ := os.Open(dobjPath) // Re-open for the test
			return DObjReader{
				path:   dobjPath,
				fpmap:  fpMap,
				filer:  file, // The filer needs to be closable
				bucket: bucket,
			}, nil
		}

		// Run the cleanup
		xlator.cleanupNamespace(ctx, namespace, mockGetDataObject)

		// Assertions
		mockMDS.AssertExpectations(t)

		// Verify the local file was deleted
		_, err = os.Stat(dobjPath)
		assert.True(t, os.IsNotExist(err), "The DObj file should have been deleted from the local cache")
	})
}
