package dedup

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	minio "github.com/minio/minio/cmd"
	redis "github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

const (
	MultipartUploadsKey         = "MultipartUploads"
	MultipartPartInfoPrefix     = "MultipartPartsInfo:"
	MultipartPartManifestPrefix = "MultipartPartsManifest:"
)

func (m *MDSRedis) GetMultipartUploadInfo(uploadID string) (objInfo minio.ObjectInfo, err error) {
	ctx := context.Background()
	val, err := m.Rdb.HGet(ctx, MultipartUploadsKey, uploadID).Result()
	if err != nil {
		return objInfo, err
	}
	if err = json.Unmarshal([]byte(val), &objInfo); err != nil {
		return objInfo, err
	}
	return objInfo, nil
}

/*
	func (m *MDSRedis) AddMultipartPart(uploadID string, partID int, partInfo minio.PartInfo, manifestList []ChunkInManifest) error {
		ctx := context.Background()
		partInfoKey := MultipartPartInfoPrefix + uploadID
		partManifestKey := MultipartPartManifestPrefix + uploadID
		partIDStr := strconv.Itoa(partID)

		partInfoJSON, err := json.Marshal(partInfo)
		if err != nil {
			return fmt.Errorf("failed to marshal part info: %w", err)
		}

		manifestJSON, err := json.Marshal(manifestList)
		if err != nil {
			return fmt.Errorf("failed to marshal part manifest: %w", err)
		}

		pipe := m.Rdb.TxPipeline()
		pipe.HSet(ctx, partInfoKey, partIDStr, partInfoJSON)
		pipe.HSet(ctx, partManifestKey, partIDStr, manifestJSON)
		_, err = pipe.Exec(ctx)
		return err
	}
*/
func (m *MDSRedis) AddMultipartPart(uploadID string, partID int, partInfo minio.PartInfo, manifestList []ChunkInManifest) error {
	ctx := context.Background()

	partIDStr := strconv.Itoa(partID)
	partInfoKey := MultipartPartInfoPrefix + uploadID
	partManifestKey := MultipartPartManifestPrefix + uploadID + ":" + partIDStr

	partInfoJSON, err := json.Marshal(partInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal part info: %w", err)
	}

	pipe := m.Rdb.TxPipeline()
	pipe.HSet(ctx, partInfoKey, partIDStr, partInfoJSON)
	//pipe.HSet(ctx, partManifestKey, partIDStr, manifestJSON)
	// Add the manifest chunks to the dedicated list for this part.
	// It's more efficient to serialize all and push them in one command within the transaction.
	if len(manifestList) > 0 {
		serializedChunks := make([]interface{}, len(manifestList))
		for i, chunk := range manifestList {
			str, err := internal.SerializeToString(chunk)
			if err != nil {
				logger.Errorf("MDSRedis::failed to SerializeToString chunk[%v] : %s", chunk, err)
				return err
			}
			serializedChunks[i] = str
		}
		// This adds the RPush command to the transaction pipeline.
		// It will be executed atomically with HSet when pipe.Exec() is called.
		pipe.RPush(ctx, partManifestKey, serializedChunks...)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// ListMultipartParts retrieves all part information and their corresponding manifests for a given uploadID.
// It first reads the central hash containing all part infos, then for each part, it fetches the manifest from its dedicated list.
func (m *MDSRedis) ListMultipartParts(uploadID string) (map[string]minio.PartInfo, map[string][]ChunkInManifest, error) {
	ctx := context.Background()

	partInfoKey := MultipartPartInfoPrefix + uploadID
	partInfoMapStr, err := m.Rdb.HGetAll(ctx, partInfoKey).Result()
	if err != nil {
		if err == redis.Nil { // If the hash doesn't exist, it's not an error, just no parts.
			return make(map[string]minio.PartInfo), make(map[string][]ChunkInManifest), nil
		}
		return nil, nil, fmt.Errorf("failed to get all part infos for upload %s: %w", uploadID, err)
	}

	infoMap := make(map[string]minio.PartInfo, len(partInfoMapStr))
	manifestMap := make(map[string][]ChunkInManifest, len(partInfoMapStr))

	for partIDStr, partInfoJSON := range partInfoMapStr {
		var pi minio.PartInfo
		if err := json.Unmarshal([]byte(partInfoJSON), &pi); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal part info for part %s: %w", partIDStr, err)
		}
		infoMap[partIDStr] = pi

		partManifestKey := MultipartPartManifestPrefix + uploadID + ":" + partIDStr
		manifestList, err := m.GetManifest(partManifestKey)
		if err != nil {
			if err == redis.Nil {
				return nil, nil, fmt.Errorf("data inconsistency: part info for part %s exists, but manifest is missing for upload %s", partIDStr, uploadID)
			}
			return nil, nil, fmt.Errorf("failed to get manifest for part %s of upload %s: %w", partIDStr, uploadID, err)
		}
		manifestMap[partIDStr] = manifestList
	}

	return infoMap, manifestMap, nil
}

func (m *MDSRedis) CleanupMultipartUpload(uploadID string) error {
	ctx := context.Background()

	partInfoKey := MultipartPartInfoPrefix + uploadID

	// Get all part IDs to find the manifest keys that need to be deleted.
	partIDs, err := m.Rdb.HKeys(ctx, partInfoKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get part keys for cleanup on upload %s: %w", uploadID, err)
	}

	// Start with the part info hash key itself.
	keysToDelete := []string{partInfoKey}

	// Add all individual part manifest keys.
	for _, partIDStr := range partIDs {
		partManifestKey := MultipartPartManifestPrefix + uploadID + ":" + partIDStr
		keysToDelete = append(keysToDelete, partManifestKey)
	}

	pipe := m.Rdb.TxPipeline()
	pipe.HDel(ctx, MultipartUploadsKey, uploadID)
	if len(keysToDelete) > 0 {
		// Delete all part-related keys.
		pipe.Del(ctx, keysToDelete...)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (m *MDSRedis) InitMultipartUpload(uploadID string, objInfo minio.ObjectInfo) error {
	ctx := context.Background()

	jsondata, err := json.Marshal(objInfo)
	if err != nil {
		logger.Errorf("InitMultipartUpload: failed to marshal objInfo for uploadID %s: %v", uploadID, err)
		return err
	}

	err = m.Rdb.HSet(ctx, MultipartUploadsKey, uploadID, jsondata).Err()
	if err != nil {
		logger.Errorf("InitMultipartUpload: failed to HSet objInfo for uploadID %s: %v", uploadID, err)
		return err
	}

	return nil
}
