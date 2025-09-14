package dedup

import (
	"context"
	"fmt"
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

	// We need to reset mocks for each subtest to avoid interference.
	newTestXlator := func() (*XlatorDedup, *MockMDS) {
		xlator.Mdsclient = new(MockMDS)
		return xlator, xlator.Mdsclient.(*MockMDS)
	}

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
		fpMap := make(map[string]BlockHeader)
		fpMap[dummyFP] = BlockHeader{Offset: 8, Len: 10}

		// Setup mock expectations
		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{doid}, nil).Once()
		mockMDS.On("GetDObjNameInMDS", doid).Return(dobjName)

		mockMDS.On("RemoveFPs", namespace, []string{dummyFP}, doid).Return(nil).Once()

		// Mock the final removal of the DOID from the deleted set
		mockMDS.On("RemoveSpecificDeletedDOIDs", namespace, []uint64{doid}).Return(nil).Once()

		// Mock the end of the GC run for this namespace
		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		// Create a mock getDataObject function for this test case.
		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			// Return a mocked DCReader
			file, _ := os.Open(dobjPath) // Re-open for the test
			return DCReader{
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

	// --- Test Case: DObj file not found ---
	t.Run("CleanupDObjNotFound", func(t *testing.T) {
		xlator, mockMDS := newTestXlator()

		doid := uint64(200)
		dobjName := "dobj-200"

		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{doid}, nil).Once()
		mockMDS.On("GetDObjNameInMDS", doid).Return(dobjName)

		// This time, getDataObject returns a "not found" error
		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			return DCReader{}, os.ErrNotExist
		}

		// We expect the DOID to be cleaned up from the GC set anyway
		mockMDS.On("RemoveSpecificDeletedDOIDs", namespace, []uint64{doid}).Return(nil).Once()
		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		xlator.cleanupNamespace(ctx, namespace, mockGetDataObject)
		mockMDS.AssertExpectations(t)
	})

	// --- Test Case: Failure to remove FPs ---
	t.Run("CleanupRemoveFPsFails", func(t *testing.T) {
		xlator, mockMDS := newTestXlator()

		doid := uint64(300)
		dobjName := "dobj-300"
		dobjPath := filepath.Join(xlator.dobjCachePath, dobjName)
		f, err := os.Create(dobjPath)
		assert.NoError(t, err)
		f.Close()

		dummyFP := "dummy-fp-for-fail-case"
		fpMap := map[string]BlockHeader{dummyFP: {}}

		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{doid}, nil).Once()
		mockMDS.On("GetDObjNameInMDS", doid).Return(dobjName)

		// Mock RemoveFPs to fail
		mockMDS.On("RemoveFPs", namespace, []string{dummyFP}, doid).Return(fmt.Errorf("redis is down")).Once()

		mockMDS.On("GetRandomDeletedDOIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			file, _ := os.Open(dobjPath)
			return DCReader{
				path:  dobjPath,
				fpmap: fpMap,
				filer: file,
			}, nil
		}

		xlator.cleanupNamespace(ctx, namespace, mockGetDataObject)
		mockMDS.AssertExpectations(t)

		// Verify the local file was NOT deleted
		_, err = os.Stat(dobjPath)
		assert.NoError(t, err, "The DObj file should NOT have been deleted")
	})
}
