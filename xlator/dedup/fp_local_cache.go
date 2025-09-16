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
	"sync"

	"github.com/zhengshuai-xiao/XlatorS/internal"
)

// FPLocalCache provides a fast, in-memory cache for fingerprint to DCID mappings.
// It is designed to be populated from a persistent store (like Redis) on startup
// and is kept in sync with writes. It is safe for concurrent use.
type FPLocalCache struct {
	mu                sync.RWMutex
	cachesByNamespace map[string]map[string]uint64 // Key: namespace, Value: map[FP] -> DCID
	mds               MDS
	xlator            *XlatorDedup
}

// NewFPLocalCache creates and initializes a new fingerprint cache.
// It populates the cache by loading all fingerprints from all manifests across all namespaces.
func NewFPLocalCache(ctx context.Context, mds MDS, xlator *XlatorDedup) (*FPLocalCache, error) {
	cache := &FPLocalCache{
		cachesByNamespace: make(map[string]map[string]uint64),
		mds:               mds,
		xlator:            xlator,
	}

	// Load initial data from all manifests in all buckets/namespaces
	if err := cache.loadAllNamespaces(ctx); err != nil {
		return nil, err
	}

	return cache, nil
}

// loadAllNamespaces orchestrates the loading of FP caches for every namespace.
func (c *FPLocalCache) loadAllNamespaces(ctx context.Context) error {
	namespaces, err := c.mds.GetAllNamespaces()
	if err != nil {
		return fmt.Errorf("FPLocalCache: failed to get all namespaces: %w", err)
	}

	allBuckets, err := c.mds.ListBuckets()
	if err != nil {
		return fmt.Errorf("FPLocalCache: failed to list all buckets: %w", err)
	}

	// Group buckets by their namespace
	bucketsByNS := make(map[string][]string)
	for _, bucketInfo := range allBuckets {
		ns, _, err := ParseNamespaceAndBucket(bucketInfo.Name)
		if err == nil {
			bucketsByNS[ns] = append(bucketsByNS[ns], bucketInfo.Name)
		}
	}

	for _, ns := range namespaces {
		c.LoadForNamespace(ctx, ns, bucketsByNS[ns])
	}
	return nil
}

// loadForNamespace scans all objects in all buckets of a given namespace,
// reads their manifests, and populates the local fingerprint cache for that namespace.
func (c *FPLocalCache) LoadForNamespace(ctx context.Context, namespace string, buckets []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fpMap := make(map[string]uint64)
	c.cachesByNamespace[namespace] = fpMap

	for _, bucket := range buckets {
		objects, err := c.mds.ListObjects(bucket, "")
		if err != nil {
			logger.Warnf("FPLocalCache: Failed to list objects in bucket %s during cache load: %v", bucket, err)
			continue // Try next bucket
		}

		for _, objInfo := range objects {
			manifestID := objInfo.UserDefined[ManifestIDKey]
			if manifestID == "" {
				continue
			}

			manifest, err := c.xlator.readManifest(ctx, namespace, manifestID)
			if err != nil {
				logger.Warnf("FPLocalCache: Failed to read manifest %s for object %s, skipping: %v", manifestID, objInfo.Name, err)
				continue
			}

			for _, chunk := range manifest {
				fpMap[chunk.FP] = chunk.DCID
			}
		}
	}

	logger.Infof("Loaded %d fingerprints into local cache for namespace %s.", len(fpMap), namespace)
}

// DedupFPsBatch checks a batch of chunks against the local cache for a given namespace.
// It updates the Deduped and DCID fields of the chunks in place.
func (c *FPLocalCache) DedupFPsBatch(namespace string, chunks []Chunk) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nsCache, ok := c.cachesByNamespace[namespace]
	if !ok {
		// If the namespace cache doesn't exist, none of the chunks can be deduped locally.
		// The caller will proceed to check the persistent MDS.
		logger.Warnf("DedupFPsBatch: local FP cache for namespace '%s' not found.", namespace)
		return nil
	}

	for i := range chunks {
		if chunks[i].Deduped {
			continue // Already deduped by a higher-level cache (e.g., intra-object)
		}

		if dcid, ok := nsCache[chunks[i].FP]; ok {
			// Found in local cache, now verify it's not a stale entry pointing to a deleted DC.
			isDeleted, err := c.mds.IsDCIDDeleted(namespace, dcid)
			if err != nil {
				// If we can't check, it's safer to not dedup and let the persistent MDS handle it.
				logger.Warnf("DedupFPsBatch: failed to check if DCID %d is deleted for fp %s: %v. Skipping local dedup.", dcid, internal.StringToHex(chunks[i].FP), err)
				continue
			}
			if isDeleted {
				// The DC is marked for deletion, so this FP is not a valid dedup target.
				logger.Tracef("DedupFPsBatch: cache hit for fp %s, but its DCID %d is marked for deletion. Skipping local dedup.", internal.StringToHex(chunks[i].FP), dcid)
			} else {
				// Valid hit.
				chunks[i].Deduped = true
				chunks[i].DCID = dcid
			}
		}
	}
	return nil
}

// InsertFPsBatch adds a batch of new fingerprint-to-DCID mappings to the local cache.
// This method only updates the in-memory cache. The caller is responsible for
// persisting these entries to the metadata store beforehand.
func (c *FPLocalCache) InsertFPsBatch(namespace string, chunks []ChunkInManifest) error {
	if len(chunks) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	nsCache, ok := c.cachesByNamespace[namespace]
	if !ok {
		nsCache = make(map[string]uint64)
		c.cachesByNamespace[namespace] = nsCache
	}
	for _, chunk := range chunks {
		nsCache[chunk.FP] = chunk.DCID
	}
	return nil
}
