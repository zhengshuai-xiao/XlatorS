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
	"io"
	"path/filepath"
	"sync"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

const (
	bufferSize = 16 * 1024 * 1024
)

var buffPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

func (x *XlatorDedup) writeObj(ctx context.Context, ns string, r *minio.PutObjReader, objInfo *minio.ObjectInfo, localFPCache map[string]uint64) (manifestList []ChunkInManifest, err error) {
	var totalSize int64 = 0
	var totalWriteSize int64 = 0
	var totalCompressedSize int64 = 0
	start := time.Now()

	// Determine chunking algorithm based on user tags.
	cdc := getCDCAlgorithm(x, objInfo.UserDefined)
	chunker, err := cdc.NewChunker(r)
	if err != nil {
		logger.Errorf("writeObj: failed to create chunker: %s", err)
		return nil, err
	}

	dcMgr, err := NewDataContainerMgr(ctx, x, ns)
	if err != nil {
		return nil, err
	}

	var chunks []Chunk

	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			logger.Errorf("writeObj: failed to get next chunk: %s", err)
			return nil, err
		}

		chunks = append(chunks, chunk)

		// Process the batch if it's large enough or if we've reached the end of the stream
		currentBatchSize := 0
		for _, c := range chunks {
			currentBatchSize += int(c.Len)
		}

		if currentBatchSize >= bufferSize || (err == io.EOF && len(chunks) > 0) {
			for _, c := range chunks {
				totalSize += int64(c.Len)
			}

			//calc fp
			CalcFPs(chunks)

			//search fp
			chunksToBatch := make([]Chunk, 0, len(chunks))
			chunkPtrsToUpdate := make(map[int]*Chunk)

			for i := range chunks {
				if doid, ok := localFPCache[chunks[i].FP]; ok {
					chunks[i].Deduped = true
					chunks[i].DOid = doid
					logger.Tracef("writeObj: local FP cache hit for fp: %s", internal.StringToHex(chunks[i].FP))
				} else {
					chunksToBatch = append(chunksToBatch, chunks[i])
					chunkPtrsToUpdate[len(chunksToBatch)-1] = &chunks[i]
				}
			}

			if len(chunksToBatch) > 0 {
				if dedupErr := x.Mdsclient.DedupFPsBatch(ns, chunksToBatch); dedupErr != nil {
					logger.Errorf("writeObj: failed to deduplicate chunks with Redis: %s", dedupErr)
					return nil, dedupErr
				}

				for i, resultChunk := range chunksToBatch {
					originalChunkPtr := chunkPtrsToUpdate[i]
					originalChunkPtr.Deduped = resultChunk.Deduped
					originalChunkPtr.DOid = resultChunk.DOid
					if resultChunk.Deduped {
						localFPCache[resultChunk.FP] = resultChunk.DOid
					}
				}
			}

			//write data
			n, compressedLen, writeErr := dcMgr.WriteChunks(chunks)
			if writeErr != nil {
				logger.Errorf("writeObj: failed to writeDObj: %s", writeErr)
				return nil, writeErr
			}
			totalWriteSize += int64(n)
			totalCompressedSize += int64(compressedLen)

			// Update local cache with newly written chunks for intra-object dedup.
			for i := range chunks {
				if !chunks[i].Deduped {
					localFPCache[chunks[i].FP] = chunks[i].DOid
				}
			}

			//append to manifest
			for _, chunk := range chunks {
				manifestList = append(manifestList, ChunkInManifest{
					FP:   chunk.FP,
					Len:  chunk.Len, // chunk.Len now correctly holds the original length
					DOid: chunk.DOid,
				})
			}

			// Reset for next batch
			chunks = nil
		}

		if err == io.EOF {
			break
		}
	}

	if err = dcMgr.Finalize(); err != nil {
		logger.Errorf("writeObj: failed to truncateDObj: %s", err)
		return
	}

	objInfo.Size = totalSize
	if totalSize > 0 {
		dedupRate := float64(totalSize-totalWriteSize) / float64(totalSize)
		compressedRate := float64(totalCompressedSize) * 100 / float64(totalSize)
		objInfo.UserDefined["wroteSize"] = fmt.Sprintf("%d", totalWriteSize)
		objInfo.UserDefined["dedupRate"] = fmt.Sprintf("%.2f%%", dedupRate*100)
		objInfo.UserDefined["compressedRate"] = fmt.Sprintf("%.2f%%", compressedRate)
	} else {
		objInfo.UserDefined["wroteSize"] = "0"
		objInfo.UserDefined["dedupRate"] = "0.00%"
		objInfo.UserDefined["compressedRate"] = "0.00%"
	}
	logger.Infof("writeObj: processed size: %d, wrote: %d, dedupRate: %s, totalCompressedSize:%d, elapsed: %s",
		totalSize, totalWriteSize, objInfo.UserDefined["dedupRate"], totalCompressedSize, time.Since(start))

	return manifestList, nil
}

func (x *XlatorDedup) writePart(ctx context.Context, ns string, r *minio.PutObjReader, objInfo minio.ObjectInfo, localFPCache map[string]uint64) (totalSize int64, totalWriteSize int64, manifestList []ChunkInManifest, err error) {
	totalWriteSize = 0
	start := time.Now()

	cdc := getCDCAlgorithm(x, objInfo.UserDefined)
	chunker, err := cdc.NewChunker(r)
	if err != nil {
		logger.Errorf("writePart: failed to create chunker: %s", err)
		return 0, 0, nil, err
	}

	dcMgr, err := NewDataContainerMgr(ctx, x, ns)
	if err != nil {
		return 0, 0, nil, err
	}

	var chunks []Chunk
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			logger.Errorf("writePart: failed to get next chunk: %s", err)
			return 0, 0, nil, err
		}

		chunks = append(chunks, chunk)

		currentBatchSize := 0
		for _, c := range chunks {
			currentBatchSize += int(c.Len)
		}

		if currentBatchSize >= bufferSize || (err == io.EOF && len(chunks) > 0) {
			for _, c := range chunks {
				totalSize += int64(c.Len)
			}

			CalcFPs(chunks)

			chunksToBatch := make([]Chunk, 0, len(chunks))
			chunkPtrsToUpdate := make(map[int]*Chunk)

			for i := range chunks {
				if doid, ok := localFPCache[chunks[i].FP]; ok {
					chunks[i].Deduped = true
					chunks[i].DOid = doid
					logger.Tracef("writePart: local FP cache hit for fp: %s", internal.StringToHex(chunks[i].FP))
				} else {
					chunksToBatch = append(chunksToBatch, chunks[i])
					chunkPtrsToUpdate[len(chunksToBatch)-1] = &chunks[i]
				}
			}

			if len(chunksToBatch) > 0 {
				if dedupErr := x.Mdsclient.DedupFPsBatch(ns, chunksToBatch); dedupErr != nil {
					logger.Errorf("writePart: failed to deduplicate chunks with Redis: %s", dedupErr)
					return 0, 0, nil, dedupErr
				}

				for i, resultChunk := range chunksToBatch {
					originalChunkPtr := chunkPtrsToUpdate[i]
					originalChunkPtr.Deduped = resultChunk.Deduped
					originalChunkPtr.DOid = resultChunk.DOid
					if resultChunk.Deduped {
						localFPCache[resultChunk.FP] = resultChunk.DOid
					}
				}
			}

			written, _, writeErr := dcMgr.WriteChunks(chunks)
			if writeErr != nil {
				logger.Errorf("writePart: failed to writeDObj: %s", writeErr)
				return 0, 0, nil, writeErr
			}
			totalWriteSize += int64(written)

			for i := range chunks {
				if !chunks[i].Deduped {
					localFPCache[chunks[i].FP] = chunks[i].DOid
				}
			}

			for _, chunk := range chunks {
				manifestList = append(manifestList, ChunkInManifest{
					FP:   chunk.FP,
					Len:  chunk.Len, // chunk.Len now correctly holds the original length
					DOid: chunk.DOid,
				})
			}
			chunks = nil
		}

		if err == io.EOF {
			break
		}
	}

	if err = dcMgr.Finalize(); err != nil {
		logger.Errorf("writePart: failed to truncateDObj: %s", err)
		return 0, 0, nil, err
	}

	var dedupRate float64
	if totalSize > 0 {
		dedupRate = float64(totalSize-totalWriteSize) / float64(totalSize)
	}
	logger.Infof("writePart: wroteSize/totalSize %d/%d bytes, dedupRate: %.2f%%, elapsed: %s",
		totalWriteSize, totalSize, dedupRate*100, time.Since(start))
	return totalSize, totalWriteSize, manifestList, nil
}

func (x *XlatorDedup) getDobjPathFromLocal(dobj_key string) string {
	//read data from object
	return filepath.Join(x.dobjCachePath, dobj_key)
}
