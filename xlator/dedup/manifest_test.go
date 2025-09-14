package dedup

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManifestOperations(t *testing.T) {
	xlator, _, teardown := setupTestXlator(t)
	defer teardown()

	ctx := context.Background()
	namespace := "test-ns"
	manifestID := "manifest-123"

	originalManifest := []ChunkInManifest{
		{FP: padFP("fp1-string-for-32-bytes-length"), Len: 1024, DOid: 1},
		{FP: padFP("fp2-string-for-32-bytes-length"), Len: 2048, DOid: 2},
		{FP: padFP("fp3-string-for-32-bytes-length"), Len: 4096, DOid: 1},
	}

	t.Run("WriteAndReadManifest", func(t *testing.T) {
		// --- Write Manifest ---
		uniqueDoids, err := xlator.writeManifest(ctx, namespace, manifestID, originalManifest)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []uint64{1, 2}, uniqueDoids)

		// Verify file exists
		path, err := xlator.getManifestPath(manifestID)
		assert.NoError(t, err)
		_, err = os.Stat(path)
		assert.NoError(t, err, "Manifest file should be created")

		// --- Read Manifest ---
		// We need to ensure the manifest is available locally for reading
		_, err = xlator.ensureManifestLocal(ctx, namespace, manifestID)
		assert.NoError(t, err)

		readManifestList, err := xlator.readManifest(ctx, namespace, manifestID)
		assert.NoError(t, err)
		assert.Equal(t, originalManifest, readManifestList)
	})

	t.Run("ReadUniqueDoids", func(t *testing.T) {
		// File should exist from previous test
		doids, err := xlator.readUniqueDoids(ctx, namespace, manifestID)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []uint64{1, 2}, doids)
	})

	t.Run("DeleteManifest", func(t *testing.T) {
		// File should exist from previous test
		err := xlator.deleteManifest(ctx, namespace, manifestID)
		assert.NoError(t, err)

		// Verify file is deleted
		path, err := xlator.getManifestPath(manifestID)
		assert.NoError(t, err)
		_, err = os.Stat(path)
		assert.True(t, os.IsNotExist(err), "Manifest file should be deleted")
	})
}
