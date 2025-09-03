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
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	S3client "github.com/zhengshuai-xiao/XlatorS/pkg/s3client"
)

const (
	bufferSize = 16 * 1024 * 1024
	maxSize    = 16 * 1024 * 1024
)

var buffPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

type fpinDObj struct {
	FP     [32]byte
	Offset uint64
	Len    uint64
}

type DObj struct {
	bucket     string
	dobj_key   string
	path       string
	dob_offset uint64
	filer      *os.File
	fps        []fpinDObj
}

type DObjReader struct {
	bucket     string
	dobj_key   string
	path       string
	dob_offset uint64
	filer      *os.File
	fpmap      map[string]fpinDObj
}

func (x *XlatorDedup) truncateDObj(ctx context.Context, dobj *DObj) (err error) {
	fps_off := dobj.dob_offset
	if fps_off <= 8 {
		logger.Tracef("truncateDObj:nothing need to write")
		//TODO:if the dedup rate is 100%, there will be a 8 bytes files generated, need to handle this scenario
		dobj.filer.Close()
		os.Remove(dobj.path)
		return nil
	}
	//write fps
	seekCurrent, _ := dobj.filer.Seek(0, io.SeekCurrent)
	logger.Tracef("xzs SeekCurrent=%d", seekCurrent)
	encoder := gob.NewEncoder(dobj.filer)
	for _, fp := range dobj.fps {
		err = encoder.Encode(fp)
		if err != nil {
			return err
		}
		seekCurrent, _ = dobj.filer.Seek(0, io.SeekCurrent)
		logger.Tracef("xzs SeekCurrent=%d", seekCurrent)
	}

	logger.Tracef("truncateDObj:write %d fps to %s", len(dobj.fps), dobj.dobj_key)
	//write offset
	b := internal.UInt64ToBytesLittleEndian(fps_off)
	_, err = dobj.filer.WriteAt(b[:], 0)
	if err != nil {
		return fmt.Errorf("failed to write fps_off[%d]: %w", fps_off, err)
	}
	dobj.filer.Close()

	// Conditionally upload DObj to S3 backend.
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		//put obj to backend
		_, err = S3client.UploadFile(ctx, x.Client, dobj.bucket, dobj.dobj_key, dobj.path)
		if err != nil {
			return fmt.Errorf("failed to upload file[%s] to S3 backend: %w", dobj.path, err)
		}
		logger.Tracef("truncateDObj: uploaded %s to S3 backend.", dobj.path)
	} else {
		logger.Tracef("truncateDObj: skipping S3 upload for %s as backend is '%s'.", dobj.path, DObjBackendPOSIX)
	}

	return nil
}
func (x *XlatorDedup) getDobjPathFromLocal(dobj_key string) string {
	//read data from object
	return filepath.Join(x.dobjCachePath, dobj_key)
}
func (x *XlatorDedup) newDObj(dobj *DObj) (err error) {
	doid, err := x.Mdsclient.GetIncreasedDOID()
	if err != nil {
		logger.Errorf("failed to GetIncreasedDOID, err:%s", err)
		return
	}
	dobj.dobj_key = x.Mdsclient.GetDObjNameInMDS(uint64(doid))
	dobj.path = x.getDobjPathFromLocal(dobj.dobj_key)
	dobj.filer, err = os.OpenFile(dobj.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	var buf_off [8]byte
	dobj.filer.Write(buf_off[:])
	dobj.dob_offset = 8
	dobj.fps = []fpinDObj{}
	logger.Tracef("newDObj, dobj_key:%s", dobj.dobj_key)

	return nil
}

func (x *XlatorDedup) writeDObj(ctx context.Context, dobj *DObj, chunks []Chunk) (int, error) {
	var writenLen int = 0
	var err error
	//for the first time, why put here, because I want to make sure there is something to write
	//just in case creating a null DObj file
	if dobj.dobj_key == "" {
		err = x.newDObj(dobj)
		if err != nil {
			return 0, err
		}
	}
	// A short-lived cache to handle duplicates within this single batch of chunks.
	// This prevents writing the same chunk multiple times into the same DObj.
	batchFPCache := make(map[string]uint64)

	for i := range chunks {
		if chunks[i].Deduped {
			continue
		}

		// Check for duplicates within this batch.
		if doid, ok := batchFPCache[chunks[i].FP]; ok {
			chunks[i].DOid = doid
			logger.Tracef("writeDObj: intra-batch FP cache hit for fp: %s", internal.StringToHex(chunks[i].FP))
			continue // Skip writing this chunk as it's a duplicate within the same batch.
		}

		if dobj.dob_offset+uint64(chunks[i].Len) >= maxSize {
			err = x.truncateDObj(ctx, dobj)
			if err != nil {
				return 0, err
			}

			err = x.newDObj(dobj)
			if err != nil {
				return 0, err
			}
		}

		len, err := internal.WriteAll(dobj.filer, chunks[i].Data)
		if err != nil {
			return 0, err
		}
		writenLen += len
		doid, err := x.Mdsclient.GetDOIDFromDObjName(dobj.dobj_key)
		if err != nil {
			return 0, err
		}
		chunks[i].DOid = uint64(doid)
		//chunks[i].OffInDOid = dobj.dob_offset
		//chunks[i].LenInDOid = chunks[i].Len

		// Cache the FP and its newly assigned DOid for this batch.
		batchFPCache[chunks[i].FP] = chunks[i].DOid
		fp := fpinDObj{
			Offset: dobj.dob_offset,
			Len:    chunks[i].Len,
		}
		copy(fp.FP[:], chunks[i].FP[:])
		dobj.fps = append(dobj.fps, fp)

		dobj.dob_offset += uint64(chunks[i].Len)

	}
	return writenLen, err
}

func (x *XlatorDedup) writeObj(ctx context.Context, ns string, r *minio.PutObjReader, objInfo *minio.ObjectInfo, localFPCache map[string]uint64) (manifestList []ChunkInManifest, err error) {
	var totalSize int64 = 0
	var totalWriteSize int64 = 0
	start := time.Now()

	// Determine chunking algorithm based on user tags.
	cdc := getCDCAlgorithm(x, objInfo.UserDefined)
	chunker, err := cdc.NewChunker(r)
	if err != nil {
		logger.Errorf("writeObj: failed to create chunker: %s", err)
		return nil, err
	}

	dobj := &DObj{bucket: GetBackendBucketName(ns), dobj_key: "", path: "", dob_offset: 0, filer: nil}

	const maxBatchSize = 16 * 1024 * 1024 // Process chunks in batches of up to 16MB
	var chunks []Chunk

	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			logger.Errorf("writeObj: failed to get next chunk: %s", err)
			return nil, err
		}

		if err != io.EOF {
			chunks = append(chunks, chunk)
		}

		// Process the batch if it's large enough or if we've reached the end of the stream
		currentBatchSize := 0
		for _, c := range chunks {
			currentBatchSize += int(c.Len)
		}

		if currentBatchSize >= maxBatchSize || (err == io.EOF && len(chunks) > 0) {
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
			n, writeErr := x.writeDObj(ctx, dobj, chunks)
			if writeErr != nil {
				logger.Errorf("writeObj: failed to writeDObj: %s", writeErr)
				return nil, writeErr
			}
			totalWriteSize += int64(n)

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
					Len:  chunk.Len,
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

	if err = x.truncateDObj(ctx, dobj); err != nil {
		logger.Errorf("writeObj: failed to truncateDObj: %s", err)
		return
	}

	objInfo.Size = totalSize
	if totalSize > 0 {
		dedupRate := float64(totalSize-totalWriteSize) / float64(totalSize)
		objInfo.UserDefined["wroteSize"] = fmt.Sprintf("%d", totalWriteSize)
		objInfo.UserDefined["dedupRate"] = fmt.Sprintf("%.2f%%", dedupRate*100)
	} else {
		objInfo.UserDefined["wroteSize"] = "0"
		objInfo.UserDefined["dedupRate"] = "0.00%"
	}
	logger.Infof("writeObj: processed size: %d, wrote: %d, dedupRate: %s, elapsed: %s",
		totalSize, totalWriteSize, objInfo.UserDefined["dedupRate"], time.Since(start))

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

	dobj := &DObj{bucket: GetBackendBucketName(ns), dobj_key: "", path: "", dob_offset: 0, filer: nil}

	const maxBatchSize = 16 * 1024 * 1024 // Process chunks in batches of up to 16MB
	var chunks []Chunk

	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			logger.Errorf("writePart: failed to get next chunk: %s", err)
			return 0, 0, nil, err
		}

		if err != io.EOF {
			chunks = append(chunks, chunk)
		}

		currentBatchSize := 0
		for _, c := range chunks {
			currentBatchSize += int(c.Len)
		}

		if currentBatchSize >= maxBatchSize || (err == io.EOF && len(chunks) > 0) {
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

			written, writeErr := x.writeDObj(ctx, dobj, chunks)
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
					Len:  chunk.Len,
					DOid: chunk.DOid,
				})
			}
			chunks = nil
		}

		if err == io.EOF {
			break
		}
	}

	if err = x.truncateDObj(ctx, dobj); err != nil {
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

func (x *XlatorDedup) parseDataObject(dobjReader *DObjReader) (err error) {
	// parse the file
	var fpOffsetStr [8]byte
	_, err = dobjReader.filer.Read(fpOffsetStr[:])
	if err != nil {
		logger.Errorf("parseDataObject: failed to read fpOffsetStr err: %s", err)
		return
	}

	fpOffset := internal.BytesToUInt64LittleEndian(fpOffsetStr)
	logger.Tracef("parseDataObject: read fpOffset: %d", fpOffset)

	//var buf []byte
	//var buf bytes.Buffer
	//buffer := make([]byte, 4096)
	dobjReader.filer.Seek(int64(fpOffset), 0)
	/*for {
		n, err := dobjReader.filer.Read(buffer)
		if n > 0 {
			buf.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		logger.Tracef("xzs 1, %d", buf.Len())
	}*/

	//buf_ := bytes.NewBufferString(string(buf[:]))
	//gob.Register(fpinDObj{})

	decoder := gob.NewDecoder(dobjReader.filer)
	for {
		var fpinDObj fpinDObj

		if err = decoder.Decode(&fpinDObj); err != nil {
			if err == io.EOF {
				break
			}
			logger.Errorf("parseDataObject: failed to DeserializeFromString [%s] err: %s", dobjReader.path, err)
			return err
		}

		dobjReader.fpmap[string(fpinDObj.FP[:])] = fpinDObj
		logger.Tracef("xzs 2:fpinDObj=(%v)", fpinDObj)
	}

	return nil
}

func (x *XlatorDedup) getDataObject(bucket, object string, o minio.ObjectOptions) (dobjReader DObjReader, err error) {
	ctx := context.Background()
	//check if the file exist
	//init the fpmap
	dobjReader.fpmap = make(map[string]fpinDObj)
	dobjReader.path = x.getDobjPathFromLocal(object)
	_, err = os.Stat(dobjReader.path)
	if err != nil {
		if os.IsNotExist(err) { // If the file does not exist on local disk
			// If the backend is S3, try to download it from there.
			if x.dsBackendType == DObjBackendS3 {
				if x.Client == nil {
					return DObjReader{}, fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
				}
				logger.Tracef("DObj %s not found in local cache, downloading from S3 backend.", dobjReader.path)
				//download from backend
				opts := miniogo.GetObjectOptions{}
				opts.ServerSideEncryption = o.ServerSideEncryption
				dobjCloseReader, _, _, err := x.Client.GetObject(ctx, bucket, object, opts)
				if err != nil {
					logger.Errorf("failed to get object[%s] from backend: %s", object, err)
					return DObjReader{}, err
				}
				_, err = internal.WriteReadCloserToFile(dobjCloseReader, dobjReader.path)
				if err != nil {
					logger.Errorf("failed to write object[%s] to local disk: %s", object, err)
					return DObjReader{}, err
				}
			} else {
				// If the backend is local and the file doesn't exist, it's an error.
				logger.Errorf("getDataObject: DObj %s not found on local disk for '%s' backend.", dobjReader.path, DObjBackendPOSIX)
				return DObjReader{}, err // err is os.IsNotExist
			}
		} else {
			logger.Errorf("failed to stat object[%s] on local disk: %s", dobjReader.path, err)
			return
		}

	}
	logger.Tracef("read data object[%s] on local disk", dobjReader.path)
	dobjReader.bucket = bucket
	dobjReader.dobj_key = object
	//open data object
	dobjReader.filer, err = os.Open(dobjReader.path)
	if err != nil {
		logger.Errorf("getDataObject: failed to open data object[%s] err: %s", dobjReader.path, err)
		return
	}

	// parse the file
	err = x.parseDataObject(&dobjReader)
	if err != nil {
		logger.Errorf("getDataObject: failed to parse data object[%s] err: %s", dobjReader.path, err)
		return
	}

	return dobjReader, nil
}
func (x *XlatorDedup) readDataObject(backendBucket string, chunks []ChunkInManifest, startOffset, length int64, writer io.Writer, o minio.ObjectOptions) (err error) {
	logger.Tracef("readDataObject enter: startOffset=%d, length=%d, totalChunks=%d", startOffset, length, len(chunks))

	var totalBytesWritten int64
	var currentObjectOffset int64

	// A map to cache DObjReaders to avoid re-opening and re-parsing the same data object file.
	dobjReaderCache := make(map[uint64]DObjReader)
	defer func() {
		for _, reader := range dobjReaderCache {
			if reader.filer != nil {
				reader.filer.Close()
			}
		}
	}()

	for _, chunk := range chunks {
		chunkLen := int64(chunk.Len)

		// If the current chunk is completely before the startOffset, skip it.
		if currentObjectOffset+chunkLen <= startOffset {
			currentObjectOffset += chunkLen
			continue
		}

		// If we have already written the required length, we can stop.
		if length != -1 && totalBytesWritten >= length {
			break
		}

		// Get or create the DObjReader for the current chunk's data object.
		dobjReader, ok := dobjReaderCache[chunk.DOid]
		if !ok {
			var newReader DObjReader
			newReader, err = x.getDataObject(backendBucket, x.Mdsclient.GetDObjNameInMDS(chunk.DOid), o)
			if err != nil {
				logger.Errorf("readDataObject: failed to get data object[%d]: %v", chunk.DOid, err)
				return err
			}
			dobjReader = newReader // assign to the loop-scoped variable
			dobjReaderCache[chunk.DOid] = dobjReader
		}

		// Find the chunk's info within the data object.
		fpinDObj, fpOk := dobjReader.fpmap[chunk.FP]
		if !fpOk {
			err = fmt.Errorf("readDataObject: fingerprint:%s not found in data object %d", internal.StringToHex(chunk.FP), chunk.DOid)
			logger.Error(err)
			return err
		}

		// Calculate how much to read from this chunk.
		offsetInChunk := int64(0)
		if startOffset > currentObjectOffset {
			offsetInChunk = startOffset - currentObjectOffset
		}

		bytesToReadInChunk := chunkLen - offsetInChunk
		if length != -1 {
			remainingLength := length - totalBytesWritten
			if bytesToReadInChunk > remainingLength {
				bytesToReadInChunk = remainingLength
			}
		}

		if bytesToReadInChunk <= 0 {
			continue
		}

		// Seek to the correct position in the data object file.
		seekPos := int64(fpinDObj.Offset) + offsetInChunk
		_, err = dobjReader.filer.Seek(seekPos, io.SeekStart)
		if err != nil {
			logger.Errorf("readDataObject: failed to seek in file %s: %v", dobjReader.path, err)
			return err
		}

		// Copy the required number of bytes from the data object file to the writer.
		n, err := io.CopyN(writer, dobjReader.filer, bytesToReadInChunk)
		if err != nil {
			logger.Errorf("readDataObject: failed to copy from file %s: %v", dobjReader.path, err)
			return err
		}

		totalBytesWritten += n
		currentObjectOffset += chunkLen
	}

	if length != -1 && totalBytesWritten < length {
		logger.Tracef("readDataObject: wrote %d bytes, which is less than requested length %d. This is normal if range exceeds object size.", totalBytesWritten, length)
	}

	logger.Tracef("readDataObject end: totalBytesWritten=%d", totalBytesWritten)
	return nil
}

func (x *XlatorDedup) readObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, objInfo minio.ObjectInfo, opts minio.ObjectOptions) (err error) {
	logger.Tracef("readObject enter")
	backendBucket := GetBackendBucketNameViaBucketName(bucket)

	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		return fmt.Errorf("readObject: failed to parse namespace from bucket %s: %w", bucket, err)
	}

	manifestID, ok := objInfo.UserDefined[ManifestIDKey]
	if !ok || manifestID == "" {
		return fmt.Errorf("manifest ID not found for object %s/%s", bucket, object)
	}

	manifest, err := x.readManifest(ctx, ns, manifestID)
	if err != nil {
		logger.Errorf("readObject: failed to get object manifest[%s] err: %s", object, err)
		return
	}
	logger.Tracef("readObject manifest=%d", len(manifest))
	err = x.readDataObject(backendBucket, manifest, startOffset, length, writer, opts)
	if err != nil {
		logger.Errorf("readObject: failed to read data object[%s] err: %s", object, err)
		return
	}
	logger.Tracef("readObject end")
	return
}
