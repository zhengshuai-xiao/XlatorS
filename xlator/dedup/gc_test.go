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

	// --- Test Case: Successfully clean up a single DCID ---
	t.Run("CleanupSingleDCID", func(t *testing.T) {
		dcid := uint64(100)
		dcName := GetDCName(dcid)
		parentDir := filepath.Join(xlator.dcCachePath, fmt.Sprintf("%d", dcid/1024))
		err := os.MkdirAll(parentDir, 0755)
		assert.NoError(t, err)
		dcPath := filepath.Join(parentDir, dcName)

		// Create a dummy DC file to be deleted
		f, err := os.Create(dcPath)
		assert.NoError(t, err, "Failed to create dummy dobj file")
		f.Close()

		// Mock the DObj content (for fpmap)
		dummyFP := "dummy-fingerprint-string-for-gc"
		fpMap := make(map[string]BlockHeader)
		fpMap[dummyFP] = BlockHeader{Offset: 8, Len: 10}

		// Setup mock expectations
		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{dcid}, nil).Once()

		mockMDS.On("RemoveFPs", namespace, []string{dummyFP}, dcid).Return(nil).Once()

		// Mock the final removal of the DOID from the deleted set
		mockMDS.On("RemoveSpecificDeletedDCIDs", namespace, []uint64{dcid}).Return(nil).Once()

		// Mock the end of the GC run for this namespace
		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		// Create a mock getDataObject function for this test case.
		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			// Return a mocked DCReader
			file, _ := os.Open(dcPath) // Re-open for the test
			return DCReader{
				path:   dcPath,
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
		_, err = os.Stat(dcPath)
		assert.True(t, os.IsNotExist(err), "The DObj file should have been deleted from the local cache")
	})

	// --- Test Case: DC file not found ---
	t.Run("CleanupDCNotFound", func(t *testing.T) {
		xlator, mockMDS := newTestXlator()

		dcid := uint64(200)

		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{dcid}, nil).Once()

		// This time, getDataObject returns a "not found" error
		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			return DCReader{}, os.ErrNotExist
		}

		// We expect the DCID to be cleaned up from the GC set anyway
		mockMDS.On("RemoveSpecificDeletedDCIDs", namespace, []uint64{dcid}).Return(nil).Once()
		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		xlator.cleanupNamespace(ctx, namespace, mockGetDataObject)
		mockMDS.AssertExpectations(t)
	})

	// --- Test Case: Failure to remove FPs ---
	t.Run("CleanupRemoveFPsFails", func(t *testing.T) {
		xlator, mockMDS := newTestXlator()

		dcid := uint64(300)
		dcName := GetDCName(dcid)
		parentDir := filepath.Join(xlator.dcCachePath, fmt.Sprintf("%d", dcid/1024))
		err := os.MkdirAll(parentDir, 0755)
		assert.NoError(t, err)
		dcPath := filepath.Join(parentDir, dcName)
		f, err := os.Create(dcPath)
		assert.NoError(t, err, "Failed to create dummy dobj file")
		f.Close()

		dummyFP := "dummy-fp-for-fail-case"
		fpMap := map[string]BlockHeader{dummyFP: {}}

		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{dcid}, nil).Once()

		// Mock RemoveFPs to fail
		mockMDS.On("RemoveFPs", namespace, []string{dummyFP}, dcid).Return(fmt.Errorf("redis is down")).Once()

		mockMDS.On("GetRandomDeletedDCIDs", namespace, int64(gcBatchSize)).Return([]uint64{}, nil).Once()

		mockGetDataObject := func(bucket, object string, o minio.ObjectOptions) (DCReader, error) {
			file, _ := os.Open(dcPath)
			return DCReader{
				path:  dcPath,
				fpmap: fpMap,
				filer: file,
			}, nil
		}

		xlator.cleanupNamespace(ctx, namespace, mockGetDataObject)
		mockMDS.AssertExpectations(t)

		// Verify the local file was NOT deleted
		_, err = os.Stat(dcPath)
		assert.NoError(t, err, "The DObj file should NOT have been deleted")
	})
}
