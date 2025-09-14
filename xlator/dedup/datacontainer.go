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
	"os"
	"path/filepath"

	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/internal/compression"
	S3client "github.com/zhengshuai-xiao/XlatorS/pkg/s3client"
)

const (
	maxContainerSize     = 16 * 1024 * 1024
	DataContainerMagic   = 0x58444346 // "XDCF" - XlatorS Data Container File
	DataContainerVersion = 1
	headerSize           = 8 // 4 bytes for magic, 4 for version
)

// DataContainer represents an active data object being written to disk.
type DataContainer struct {
	bucket string
	key    string
	path   string
	offset uint64
	filer  *os.File
	fps    []BlockHeader
}

// DataContainerMgr manages the lifecycle of data containers for a write operation.
type DataContainerMgr struct {
	xlator          *XlatorDedup
	ctx             context.Context
	ns              string
	activeContainer *DataContainer
	compressor      compression.Compressor
}

// NewDataContainerMgr creates a new manager for handling data container writes.
func NewDataContainerMgr(ctx context.Context, xlator *XlatorDedup, ns string) (*DataContainerMgr, error) {
	compressor, err := compression.GetCompressorViaString(xlator.Compression)
	if err != nil {
		return nil, err
	}
	return &DataContainerMgr{
		xlator:     xlator,
		ctx:        ctx,
		ns:         ns,
		compressor: compressor,
	}, nil
}

// WriteChunks writes a batch of new (not deduped) chunks. It handles container creation,
// writing data, and rolling over to a new container when full.
func (mgr *DataContainerMgr) WriteChunks(chunks []Chunk) (writtenLen int, compressedLen int, err error) {
	if mgr.activeContainer == nil {
		// Lazily create the first container only when there's data to write.
		hasNewData := false
		for _, c := range chunks {
			if !c.Deduped {
				hasNewData = true
				break
			}
		}
		if !hasNewData {
			return 0, 0, nil
		}

		mgr.activeContainer, err = mgr.newContainer()
		if err != nil {
			return 0, 0, err
		}
	}

	batchFPCache := make(map[string]uint64)

	for i := range chunks {
		if chunks[i].Deduped {
			continue
		}

		if doid, ok := batchFPCache[chunks[i].FP]; ok {
			chunks[i].DOid = doid
			continue
		}

		if mgr.activeContainer.offset+uint64(chunks[i].Len) >= maxContainerSize {
			if err = mgr.finalizeContainer(mgr.activeContainer); err != nil {
				return 0, 0, err
			}
			mgr.activeContainer, err = mgr.newContainer()
			if err != nil {
				return 0, 0, err
			}
		}

		dataToWrite := chunks[i].Data
		if mgr.compressor != nil {
			compressedData, err := mgr.compressor.Compress(chunks[i].Data)
			if err != nil {
				return 0, 0, err
			}
			compressedLen += (len(chunks[i].Data) - len(compressedData))
			dataToWrite = compressedData
		}

		wlen, err := internal.WriteAll(mgr.activeContainer.filer, dataToWrite)
		if err != nil {
			return 0, 0, err
		}
		writtenLen += wlen

		doid, err := mgr.xlator.Mdsclient.GetDOIDFromDObjName(mgr.activeContainer.key)
		if err != nil {
			return 0, 0, err
		}
		chunks[i].DOid = uint64(doid)
		batchFPCache[chunks[i].FP] = chunks[i].DOid

		fp := BlockHeader{
			Offset: mgr.activeContainer.offset,
			Len:    uint64(len(dataToWrite)),
			CRC:    0, // TODO
		}
		if mgr.compressor != nil {
			fp.CompressType = byte(mgr.compressor.Type())
		}
		copy(fp.FP[:], chunks[i].FP[:])
		mgr.activeContainer.fps = append(mgr.activeContainer.fps, fp)
		mgr.activeContainer.offset += uint64(len(dataToWrite))
	}
	return writtenLen, compressedLen, nil
}

// Finalize ensures any active data container is properly closed and uploaded.
func (mgr *DataContainerMgr) Finalize() error {
	if mgr.activeContainer != nil {
		return mgr.finalizeContainer(mgr.activeContainer)
	}
	return nil
}

// newContainer creates and initializes a new data container file.
func (mgr *DataContainerMgr) newContainer() (*DataContainer, error) {
	doid, err := mgr.xlator.Mdsclient.GetIncreasedDOID()
	if err != nil {
		return nil, err
	}
	key := mgr.xlator.Mdsclient.GetDObjNameInMDS(uint64(doid))
	path := filepath.Join(mgr.xlator.dobjCachePath, key)
	filer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	// Write magic number and version
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], DataContainerMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(DataContainerVersion))
	if _, err := filer.Write(header); err != nil {
		filer.Close()
		return nil, fmt.Errorf("failed to write data container header: %w", err)
	}

	return &DataContainer{
		bucket: GetBackendBucketName(mgr.ns),
		key:    key,
		path:   path,
		filer:  filer,
		offset: headerSize,
		fps:    []BlockHeader{},
	}, nil
}

// finalizeContainer writes metadata to the container file, closes it, and uploads it.
func (mgr *DataContainerMgr) finalizeContainer(container *DataContainer) error {
	defer container.filer.Close()
	// If only the header was written, it's an empty container.
	if container.offset == headerSize {
		os.Remove(container.path)
		return nil
	}

	// The current offset is where the metadata (gob) will start.
	metadataOffset := container.offset

	encoder := gob.NewEncoder(container.filer)
	for _, fp := range container.fps {
		if err := encoder.Encode(fp); err != nil {
			return err
		}
	}

	// Now write the offset of the metadata at the very end of the file.
	offsetBytes := internal.UInt64ToBytesLittleEndian(metadataOffset)
	if _, err := container.filer.Write(offsetBytes[:]); err != nil {
		return fmt.Errorf("failed to write offset footer: %w", err)
	}

	if mgr.xlator.dsBackendType == DObjBackendS3 {
		if _, err := S3client.UploadFile(mgr.ctx, mgr.xlator.Client, container.bucket, container.key, container.path); err != nil {
			return fmt.Errorf("failed to upload data container %s: %w", container.path, err)
		}
	}
	return nil
}
