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
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/internal/compression"
)

// BlockHeader defines the metadata for a single data block stored within a data container.
// It is written to the footer of the data container file.
type BlockHeader struct {
	FP           [32]byte
	Offset       uint64
	Len          uint64
	CRC          uint32
	CompressType byte
}

// DCReader (Data Container Reader) provides read access to a data container.
// It holds the file handle and a map of fingerprints to their block headers.
type DCReader struct {
	bucket string
	dc_key string
	path   string
	filer  *os.File
	fpmap  map[string]BlockHeader
}

// readChunkData reads a specific segment of a data block from the data container file.
// It handles both compressed and uncompressed data.
func (dr *DCReader) readChunkData(blockHeader BlockHeader, offsetInChunk, bytesToRead int64, writer io.Writer) (int64, error) {
	compressor, err := compression.GetCompressorViaType(compression.CompressionType(blockHeader.CompressType))
	if err != nil {
		logger.Errorf("readChunkData: failed to get compressor: %v", err)
		return 0, err
	}

	var n int64
	if compressor != nil {
		// For compressed chunks, read the whole compressed block first.
		// This is a simplification; for large chunks, streaming decompression would be better.
		compressedBuffer := make([]byte, blockHeader.Len)
		_, err := dr.filer.ReadAt(compressedBuffer, int64(blockHeader.Offset))
		if err != nil {
			logger.Errorf("readChunkData: failed to read compressed chunk from %s: %v", dr.path, err)
			return 0, err
		}

		decompressedBuf, err := compressor.Decompress(compressedBuffer)
		if err != nil {
			logger.Errorf("readChunkData: failed to decompress: %v", err)
			return 0, err
		}

		// The length of decompressedBuf is the original chunk length.
		// Ensure the slice operation is safe.
		endOffset := offsetInChunk + bytesToRead
		if endOffset > int64(len(decompressedBuf)) {
			return 0, fmt.Errorf("readChunkData: slice bounds out of range. offsetInChunk=%d, bytesToRead=%d, decompressedLen=%d", offsetInChunk, bytesToRead, len(decompressedBuf))
		}

		written, err := writer.Write(decompressedBuf[offsetInChunk:endOffset])
		if err != nil {
			logger.Errorf("readChunkData: failed to write decompressed buf from %s: %v", dr.path, err)
			return 0, err
		}
		n = int64(written)
	} else {
		// For uncompressed data, we can seek directly into the chunk and read the required range.
		seekPos := int64(blockHeader.Offset) + offsetInChunk
		sectionReader := io.NewSectionReader(dr.filer, seekPos, bytesToRead)

		n, err = io.Copy(writer, sectionReader)
		if err != nil {
			logger.Errorf("readChunkData: failed to read from file %s: %v", dr.path, err)
			return 0, err
		}
	}

	if n != bytesToRead {
		return n, fmt.Errorf("readChunkData: short read: expected to read %d bytes, but got %d", bytesToRead, n)
	}

	return n, nil
}

// parseDataContainer reads the footer of a data container file, decodes the block headers,
// and populates the DCReader's fpmap.
func (dr *DCReader) parseDataContainer() error {
	// The metadata offset is stored in the last 8 bytes of the file.
	fi, err := dr.filer.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat data container %s: %w", dr.path, err)
	}
	fileSize := fi.Size()
	if fileSize < headerSize+8 { // Must contain at least header and metadata offset
		return fmt.Errorf("data container %s is too small to be valid", dr.path)
	}

	// Read and verify header
	header := make([]byte, headerSize)
	if _, err := dr.filer.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read header from %s: %w", dr.path, err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	version := binary.LittleEndian.Uint32(header[4:8])

	if magic != DataContainerMagic {
		return fmt.Errorf("invalid magic number in %s: got %x, want %x", dr.path, magic, DataContainerMagic)
	}

	if version != DataContainerVersion {
		return fmt.Errorf("unsupported data container version in %s: got %d, want %d", dr.path, version, DataContainerVersion)
	}

	offsetBuf := make([]byte, 8)
	if _, err := dr.filer.ReadAt(offsetBuf, fileSize-8); err != nil { // Read last 8 bytes
		return fmt.Errorf("failed to read metadata offset from %s: %w", dr.path, err)
	}

	fpOffsetStr := [8]byte{}
	copy(fpOffsetStr[:], offsetBuf)
	fpOffset := internal.BytesToUInt64LittleEndian(fpOffsetStr)
	logger.Tracef("parseDataContainer: read fpOffset: %d", fpOffset)

	decoder := gob.NewDecoder(io.NewSectionReader(dr.filer, int64(fpOffset), fileSize-int64(fpOffset)-8))
	for {
		var blockHeader BlockHeader

		if err = decoder.Decode(&blockHeader); err != nil {
			if err == io.EOF {
				break
			}
			logger.Errorf("parseDataContainer: failed to DeserializeFromString [%s] err: %s", dr.path, err)
			return err
		}

		dr.fpmap[string(blockHeader.FP[:])] = blockHeader
		logger.Tracef("xzs 2:blockHeader=(%v)", blockHeader)
	}

	return nil
}

// getDataObject retrieves a data container, making it available for reading.
// It first checks the local cache and, if not found, downloads it from the S3 backend.
// It then opens the file and parses its metadata into a DCReader.
func (x *XlatorDedup) getDataObject(bucket, object string, o minio.ObjectOptions) (dcReader DCReader, err error) {
	ctx := context.Background()

	dcid, err := GetDCIDFromDCName(object)
	if err != nil {
		return DCReader{}, fmt.Errorf("getDataObject: failed to get DOID from object name %s: %w", object, err)
	}

	dcReader.fpmap = make(map[string]BlockHeader)

	// This single call handles both POSIX (check existence) and S3 (download if not in cache).
	localPath, err := x.dcBackend.Download(ctx, bucket, uint64(dcid))
	if err != nil {
		// If the file is not found either locally (POSIX) or remotely (S3), we'll get an error here.
		logger.Errorf("getDataObject: failed to download/find data container for DCID %d: %v", dcid, err)
		return DCReader{}, err
	}
	dcReader.path = localPath
	logger.Tracef("read data object[%s] on local disk", dcReader.path)
	dcReader.bucket = bucket
	dcReader.dc_key = object
	//open data object
	dcReader.filer, err = os.Open(dcReader.path)
	if err != nil {
		logger.Errorf("getDataObject: failed to open data object[%s] err: %s", dcReader.path, err)
		return
	}

	// parse the file
	err = dcReader.parseDataContainer()
	if err != nil {
		logger.Errorf("getDataObject: failed to parse data object[%s] err: %s", dcReader.path, err)
		return
	}

	return dcReader, nil
}

// readDataObject reconstructs object data by reading chunks from various data containers based on the manifest.
func (x *XlatorDedup) readDataObject(backendBucket string, chunks []ChunkInManifest, startOffset, length int64, writer io.Writer, o minio.ObjectOptions) (err error) {
	logger.Tracef("readDataObject enter: startOffset=%d, length=%d, totalChunks=%d", startOffset, length, len(chunks))

	var totalBytesWritten int64
	var currentObjectOffset int64

	// A map to cache DCReaders to avoid re-opening and re-parsing the same data object file.
	dcReaderCache := make(map[uint64]DCReader)
	defer func() {
		for _, reader := range dcReaderCache {
			if reader.filer != nil {
				reader.filer.Close()
			}
		}
	}()

	for _, chunk := range chunks {
		chunkLen := int64(chunk.Len)

		if currentObjectOffset+chunkLen <= startOffset {
			currentObjectOffset += chunkLen
			continue
		}

		if length != -1 && totalBytesWritten >= length {
			break
		}

		dcReader, ok := dcReaderCache[chunk.DCID]
		if !ok {
			var newReader DCReader
			newReader, err = x.getDataObject(backendBucket, GetDCName(chunk.DCID), o)
			if err != nil {
				logger.Errorf("readDataObject: failed to get data container[%d]: %v", chunk.DCID, err)
				return err
			}
			dcReader = newReader
			dcReaderCache[chunk.DCID] = dcReader
		}

		blockHeader, fpOk := dcReader.fpmap[chunk.FP]
		if !fpOk {
			err = fmt.Errorf("readDataObject: fingerprint:%s not found in data container %d", internal.StringToHex(chunk.FP), chunk.DCID)
			logger.Error(err)
			return err
		}

		var offsetInChunk int64
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
			currentObjectOffset += chunkLen
			continue
		}

		n, readErr := dcReader.readChunkData(blockHeader, offsetInChunk, bytesToReadInChunk, writer)
		if readErr != nil {
			return readErr
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

// readObject is the high-level function for reading an object's content.
// It retrieves the manifest and then orchestrates the data reconstruction via readDataObject.
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
