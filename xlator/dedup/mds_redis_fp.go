// Copyright 2025 zhengshuai.xiao@outlook.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
package dedup

import (
	"context"
	"fmt"
	"strconv"

	redis "github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

// LoadFPCache scans the entire fingerprint hash for a given namespace in Redis
// and returns it as an in-memory map. This is used to populate a local cache on startup.
func (m *MDSRedis) LoadFPCache(namespace string) (map[string]uint64, error) {
	fpMap := make(map[string]uint64)
	ctx := context.Background()
	fpCacheKey := GetFingerprintCache(namespace)

	// Use HSCAN to iterate over the keys without blocking the Redis server for too long.
	iter := m.Rdb.HScan(ctx, fpCacheKey, 0, "*", 1000).Iterator()
	for iter.Next(ctx) {
		// The iterator returns key, then value, in pairs.
		fp := iter.Val()
		if !iter.Next(ctx) {
			break // Should not happen with a valid HASH
		}
		dcidStr := iter.Val()

		dcid, err := strconv.ParseUint(dcidStr, 10, 64)
		if err != nil {
			logger.Warnf("Failed to parse DCID '%s' for FP '%s' from Redis, skipping: %v", dcidStr, fp, err)
			continue
		}
		fpMap[fp] = dcid
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	logger.Infof("Loaded %d fingerprints from namespace '%s' into local cache from Redis.", len(fpMap), namespace)
	return fpMap, nil
}

func (m *MDSRedis) DedupFPs(namespace string, chunks []Chunk) error {
	//pipe := m.rdb.Pipeline()

	for i, chunk := range chunks {

		fps := m.getFingerprint(namespace, chunk.FP)
		if fps != nil {
			chunks[i].DCID = fps.DCID
			chunks[i].Deduped = true
			//chunks[i].LenInDOid = fps.LenInDOid
			//chunks[i].OffInDOid = fps.OffInDOid
			logger.Tracef("DedupFPs:found existed fp:%s", internal.StringToHex(chunk.FP))
		}
	}
	return nil
}

func (m *MDSRedis) InsertFPs(namespace string, chunks []ChunkInManifest) error {
	//pipe := m.rdb.Pipeline()

	for _, chunk := range chunks {
		//pipe.Set(ctx, getFPNameInMDS(chunk.FP), str, 0)
		err := m.setFingerprint(namespace, chunk.FP, FPValInMDS{DCID: chunk.DCID})
		if err != nil {
			return err
		}
		logger.Tracef("InsertFPs: fp[%s], DCID:%d", internal.StringToHex(chunk.FP), chunk.DCID)
	}
	//_, err := pipe.Exec(ctx)
	return nil
}

func (m *MDSRedis) DedupFPsBatch(namespace string, chunks []Chunk) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	deleteDCIDKey := GetDeletedDCIDKey(namespace)
	// Use a WATCH transaction to ensure we get a consistent view of the fingerprints.
	// If the fingerprint cache is modified concurrently (e.g., by RemoveFPs),
	// the transaction will fail and retry, preventing decisions based on stale data.
	err := m.Rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(chunks) == 0 {
			return nil
		}

		fps := make([]string, len(chunks))
		for i := range chunks {
			fps[i] = chunks[i].FP
		}

		// HMGet is more efficient for batch lookups.
		vals, err := tx.HMGet(ctx, fpCache, fps...).Result()
		if err != nil {
			// redis.Nil is not returned by HMGet. Instead, the slice contains nil for non-existent keys.
			return err
		}

		for i := range chunks {
			if vals[i] == nil {
				// Fingerprint not found in cache.
				chunks[i].Deduped = false
				continue
			}

			doidStr, ok := vals[i].(string)
			if !ok || doidStr == "" {
				// This case should ideally not happen if data is consistent.
				logger.Warnf("DedupFPsBatch: unexpected or empty value for fp %s", internal.StringToHex(chunks[i].FP))
				chunks[i].Deduped = false
				continue
			}

			DCID, err := strconv.ParseUint(doidStr, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse DCID '%s' for fp %s: %w", doidStr, internal.StringToHex(chunks[i].FP), err)
			}
			exist, err := m.IsDCIDDeleted(namespace, DCID)
			if err != nil {
				return err
			}
			if exist {
				logger.Tracef("the fp[%s]'s Data container[id:%d] is in deleted list, so skip it", internal.StringToHex(chunks[i].FP), DCID)
				chunks[i].Deduped = false
				continue
			}
			chunks[i].DCID = DCID
			chunks[i].Deduped = true
			logger.Tracef("DedupFPsBatch: found existing fp:%s in %s, DCID: %d", internal.StringToHex(chunks[i].FP), fpCache, DCID)
		}
		return nil
	}, fpCache, deleteDCIDKey)

	if err != nil {
		logger.Errorf("DedupFPsBatch: transaction failed for namespace %s: %v", namespace, err)
	}
	return err
}

func (m *MDSRedis) InsertFPsBatch(namespace string, chunks []ChunkInManifest) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)

	// Use a WATCH transaction to prevent race conditions with concurrent deletes or inserts.
	err := m.Rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(chunks) == 0 {
			return nil
		}
		pipe := tx.TxPipeline()
		for _, chunk := range chunks {
			// HSetNX is used to avoid overwriting a fingerprint that might have been
			// inserted by a concurrent process.
			// Using HSet will overwrite any existing value,
			// which can lead to race conditions and data inconsistency.
			//But here we should use HSet
			pipe.HSet(ctx, fpCache, chunk.FP, strconv.FormatUint(chunk.DCID, 10))
			logger.Tracef("InsertFPsBatch: fp[%s], DCID:%d", internal.StringToHex(chunk.FP), chunk.DCID)
		}
		_, err := pipe.Exec(ctx)
		return err
	}, fpCache)

	if err != nil {
		logger.Errorf("InsertFPsBatch: transaction failed for namespace %s: %v", namespace, err)
	}
	return err
}

// RemoveFPs removes a batch of fingerprints from the cache for a specific namespace.
// It only removes a fingerprint if its associated Data Object ID matches the provided DOid.
// This is crucial to prevent deleting a fingerprint that has been reused by a newer Data Object.
// This operation is atomic, using a Redis WATCH transaction to effectively lock the fingerprint
// cache for the given namespace during the operation.
func (m *MDSRedis) RemoveFPs(namespace string, FPs []string, DCID uint64) error {
	ctx := context.Background()
	fpCacheKey := GetFingerprintCache(namespace)
	dcidStr := strconv.FormatUint(DCID, 10)

	err := m.Rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(FPs) == 0 {
			return nil
		}

		// Get all current DOIDs for the given FPs in one command to be efficient.
		storedDoidVals, err := tx.HMGet(ctx, fpCacheKey, FPs...).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		var fpsToDelete []string
		for i, fp := range FPs {
			// storedDoidVals[i] will be nil if the key does not exist in the hash.
			if storedDoidVals[i] == nil {
				logger.Warnf("RemoveFPs: fingerprint %s not found in cache %s, skipping.", internal.StringToHex(fp), fpCacheKey)
				continue
			}

			storedDoidStr, ok := storedDoidVals[i].(string)
			if !ok {
				// This should not happen with Redis string values.
				logger.Errorf("RemoveFPs: unexpected type for fingerprint %s value in cache %s", internal.StringToHex(fp), fpCacheKey)
				continue
			}

			// Only add the FP to the deletion list if the stored DOid matches the expected one.
			if storedDoidStr == dcidStr {
				fpsToDelete = append(fpsToDelete, fp)
			} else {
				// This is a valid scenario where the fingerprint has been overwritten by a new
				// data object. We must not delete it.
				logger.Warnf("RemoveFPs: fingerprint %s in cache %s has a different DCID (%s) than expected (%s). Not deleting.", internal.StringToHex(fp), fpCacheKey, storedDoidStr, dcidStr)
			}
		}

		if len(fpsToDelete) > 0 {
			// Use a transaction pipeline to delete all matching FPs at once.
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HDel(ctx, fpCacheKey, fpsToDelete...)
				return nil
			})
			return err
		}

		return nil
	}, fpCacheKey)

	if err != nil {
		logger.Errorf("RemoveFPs: transaction failed for namespace %s: %v", namespace, err)
	}

	return err
}

func (m *MDSRedis) getFingerprint(namespace string, fp string) *FPValInMDS {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	dcidStr, err := m.Rdb.HGet(ctx, fpCache, fp).Result()
	if err != nil {
		if err == redis.Nil {
			//not existed
			return nil
		}
		logger.Errorf("getFingerprint: failed to HGet(%s). err:%s", internal.StringToHex(fp), err)
		return nil
	}
	fps := &FPValInMDS{}
	fps.DCID, err = strconv.ParseUint(dcidStr, 10, 64)
	if err != nil {
		logger.Errorf("getFingerprint: failed to ParseUint. err:%s", err)
		return nil
	}
	logger.Tracef("found fp:%s", internal.StringToHex(fp))
	return fps
}

func (m *MDSRedis) setFingerprint(namespace string, fp string, dpval FPValInMDS) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	str_val, err := internal.SerializeToString(dpval)
	if err != nil {
		logger.Errorf("setFingerprint: failed to SerializeToString. err:%s", err)
		return err
	}
	return m.Rdb.HSet(ctx, fpCache, fp, str_val).Err()
}
