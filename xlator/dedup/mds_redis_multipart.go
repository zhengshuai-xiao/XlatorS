package dedup

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	minio "github.com/minio/minio/cmd"
	redis "github.com/redis/go-redis/v9"
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

func (m *MDSRedis) ListMultipartParts(uploadID string) (map[string]minio.PartInfo, map[string][]ChunkInManifest, error) {
	ctx := context.Background()
	partInfoKey := MultipartPartInfoPrefix + uploadID
	partManifestKey := MultipartPartManifestPrefix + uploadID

	infoMapStr, err := m.Rdb.HGetAll(ctx, partInfoKey).Result()
	if err != nil && err != redis.Nil {
		return nil, nil, err
	}
	infoMap := make(map[string]minio.PartInfo, len(infoMapStr))
	for k, v := range infoMapStr {
		var pi minio.PartInfo
		if err := json.Unmarshal([]byte(v), &pi); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal part info for part %s: %w", k, err)
		}
		infoMap[k] = pi
	}

	manifestMapStr, err := m.Rdb.HGetAll(ctx, partManifestKey).Result()
	if err != nil && err != redis.Nil {
		return nil, nil, err
	}
	manifestMap := make(map[string][]ChunkInManifest, len(manifestMapStr))
	for k, v := range manifestMapStr {
		var cim []ChunkInManifest
		if err := json.Unmarshal([]byte(v), &cim); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal part manifest for part %s: %w", k, err)
		}
		manifestMap[k] = cim
	}

	return infoMap, manifestMap, nil
}

func (m *MDSRedis) CleanupMultipartUpload(uploadID string) error {
	ctx := context.Background()
	partInfoKey := MultipartPartInfoPrefix + uploadID
	partManifestKey := MultipartPartManifestPrefix + uploadID

	pipe := m.Rdb.TxPipeline()
	pipe.HDel(ctx, MultipartUploadsKey, uploadID)
	pipe.Del(ctx, partInfoKey, partManifestKey)
	_, err := pipe.Exec(ctx)
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
