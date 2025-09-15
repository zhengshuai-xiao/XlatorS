package dedup

import (
	"context"
	"os"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	minio "github.com/minio/minio/cmd"
)

// ForceGarbageCollection triggers a garbage collection cycle immediately.
// This is intended for manual administrative actions.
// It runs the GC process in a background goroutine to avoid blocking the caller
// and ensures it is tracked by the WaitGroup for graceful shutdown.
func (x *XlatorDedup) ForceGarbageCollection() {
	logger.Info("Manual GC run triggered via admin call.")
	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		logger.Info("Executing manual GC run.")
		x.runGC()
		logger.Info("Manual GC run finished.")
	}()
}

const (
	adminCommandChannel = "dedup:admin:commands"
	triggerGCCommand    = "TRIGGER_GC"
)

// listenForAdminCommands starts a goroutine that listens on a Redis Pub/Sub channel for administrative commands.
func (x *XlatorDedup) listenForAdminCommands() {
	// Type assert Mdsclient to access the underlying Redis client.
	mdsRedis, ok := x.Mdsclient.(*MDSRedis)
	if !ok { //TODO:
		logger.Error("Cannot start admin command listener: Mdsclient is not of type *MDSRedis")
		return
	}

	pubsub := mdsRedis.Rdb.Subscribe(context.Background(), adminCommandChannel)

	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		defer pubsub.Close()

		logger.Infof("Admin command listener started on Redis channel '%s'.", adminCommandChannel)
		ch := pubsub.Channel()

		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					logger.Warn("Admin command channel closed. Listener is stopping.")
					return
				}
				logger.Infof("Received admin command: %s", msg.Payload)
				if msg.Payload == triggerGCCommand {
					x.ForceGarbageCollection()
				}
			case <-x.stopGC:
				logger.Info("Stopping admin command listener.")
				return
			}
		}
	}()
}

const (
	gcInterval  = 1 * time.Hour // How often to run the GC
	gcBatchSize = 100           // How many DOIDs to process per run per namespace
)

// startGC launches the background garbage collection goroutine.
func (x *XlatorDedup) startGC() {
	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		logger.Info("Starting background GC worker...")
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Info("GC run triggered.")
				x.runGC()
			case <-x.stopGC:
				logger.Info("Stopping background GC worker...")
				return
			}
		}
	}()
}

// runGC performs a single garbage collection cycle.
func (x *XlatorDedup) runGC() {
	ctx := context.Background()
	namespaces, err := x.Mdsclient.GetAllNamespaces()
	if err != nil {
		logger.Errorf("GC: failed to get all namespaces: %v", err)
		return
	}
	if len(namespaces) == 0 {
		logger.Info("GC: no namespaces found to process.")
		return
	}

	for _, ns := range namespaces {
		logger.Infof("GC: processing namespace %s", ns)
		x.cleanupNamespace(ctx, ns, x.getDataObject)
	}
}

// cleanupNamespace processes the deleted DOID queue for a single namespace.
func (x *XlatorDedup) cleanupNamespace(ctx context.Context, namespace string, getDataObjectFunc func(bucket, object string, o minio.ObjectOptions) (DCReader, error)) {
	backendBucket := GetBackendBucketName(namespace)

	for {
		// Step 1: Get a batch of DOIDs without removing them from the set to avoid race conditions.
		dcids, err := x.Mdsclient.GetRandomDeletedDCIDs(namespace, gcBatchSize)
		if err != nil {
			logger.Errorf("GC: failed to get deleted DCIDs for namespace %s: %v", namespace, err)
			return
		}
		if len(dcids) == 0 {
			logger.Infof("GC: no more DCIDs to process for namespace %s.", namespace)
			return
		}

		logger.Infof("GC: processing %d DCIDs for namespace %s.", len(dcids), namespace)

		var successfullyCleanedDCIDs []uint64
		for _, dcid := range dcids {
			dcName := GetDCName(dcid)

			// 1. Get FPs from data object
			dcReader, err := getDataObjectFunc(backendBucket, dcName, minio.ObjectOptions{})
			if err != nil {
				resp, isS3Err := err.(miniogo.ErrorResponse)
				// If the object is not found (either on S3 or local disk), we can assume it's already been cleaned up.
				if (isS3Err && resp.Code == "NoSuchKey") || os.IsNotExist(err) {
					logger.Warnf("GC: data container for DCID %d not found. Assuming already deleted.", dcid)
					// This DOID can be removed from the GC set.
					successfullyCleanedDCIDs = append(successfullyCleanedDCIDs, dcid)
				} else {
					// For any other error, log it and retry later.
					logger.Errorf("GC: failed to get data container %s/%s: %v. Will retry later.", backendBucket, dcName, err)
				}
				continue // Move to the next DOID
			}

			// 2. Remove FPs from cache
			if dcReader.fpmap != nil {
				fps := make([]string, 0, len(dcReader.fpmap))
				for fpStr := range dcReader.fpmap {
					fps = append(fps, fpStr)
				}
				if len(fps) > 0 {
					if err := x.Mdsclient.RemoveFPs(namespace, fps, dcid); err != nil {
						logger.Errorf("GC: failed to remove FPs for DCID %d: %v. Will retry later.", dcid, err)
						// The file handle will be closed by the deferred call below.
						continue // Move to the next DOID
					}
				}
			}

			// Defer the closing of the file handle to ensure it's closed on all paths for this DOID.
			if dcReader.filer != nil { // nolint:ifshort
				defer dcReader.filer.Close()
			}

			// 3. Delete data object from backend
			err = x.dcBackend.Delete(ctx, backendBucket, dcid)
			if err != nil && !os.IsNotExist(err) {
				logger.Errorf("GC: failed to delete data container for DCID %d from backend: %v. Will retry later.", dcid, err)
				continue
			}

			logger.Infof("GC: successfully processed DCID %d for cleanup.", dcid)
			successfullyCleanedDCIDs = append(successfullyCleanedDCIDs, dcid)
		}

		// Step 2: After processing the batch, remove the successfully cleaned DOIDs from the set.
		if len(successfullyCleanedDCIDs) > 0 {
			if err := x.Mdsclient.RemoveSpecificDeletedDCIDs(namespace, successfullyCleanedDCIDs); err != nil {
				logger.Errorf("GC: CRITICAL: failed to remove %d processed DCIDs from set for namespace %s: %v", len(successfullyCleanedDCIDs), namespace, err)
				// These DOIDs will be re-processed, which is safe but inefficient.
			} else {
				logger.Infof("GC: successfully removed %d processed DCIDs from GC set for namespace %s.", len(successfullyCleanedDCIDs), namespace)
			}
		}
	}
}
