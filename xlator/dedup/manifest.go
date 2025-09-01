// Copyright 2024 zhengshuai.xiao@outlook.com
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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/zhengshuai-xiao/XlatorS/internal"
)

// getManifestPath generates a sharded file path for a manifest based on its ID.
// e.g., manifestID "abcdef1234" -> <cache_path>/manifests/ab/cd/abcdef1234
func (x *XlatorDedup) getManifestPath(manifestID string) (string, error) {
	if len(manifestID) < 4 {
		return "", fmt.Errorf("invalid manifest ID: %s", manifestID)
	}
	dir := filepath.Join(x.dobjCachePath, "manifests")
	return filepath.Join(dir, manifestID), nil
}

// writeManifestToFile serializes a manifest and writes it to a file.
// The file format is:
// [8-byte offset to DOID set] [ChunkInManifest entries...] [Unique DOID set...]
func (x *XlatorDedup) writeManifestToFile(manifestID string, manifestList []ChunkInManifest) ([]uint64, error) {
	path, err := x.getManifestPath(manifestID)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest path: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, fmt.Errorf("failed to create manifest directory: %w", err)
	}

	var buf bytes.Buffer
	uniqueDoids := internal.NewUInt64Set()

	for _, chunk := range manifestList {
		// FP (32 bytes)
		buf.WriteString(chunk.FP)
		// Len (8 bytes)
		lenBytes := internal.UInt64ToBytesLittleEndian(chunk.Len)
		buf.Write(lenBytes[:])
		// DOid (8 bytes)
		doidBytes := internal.UInt64ToBytesLittleEndian(chunk.DOid)
		buf.Write(doidBytes[:])

		uniqueDoids.Add(chunk.DOid)
	}

	dobjIdOffset := uint64(buf.Len())

	uniqueDoidList := uniqueDoids.Elements()
	for _, doid := range uniqueDoidList {
		doidBytes := internal.UInt64ToBytesLittleEndian(doid)
		buf.Write(doidBytes[:])
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest file for writing: %w", err)
	}
	defer file.Close()

	offsetHeader := internal.UInt64ToBytesLittleEndian(dobjIdOffset)
	if _, err := file.Write(offsetHeader[:]); err != nil {
		return nil, fmt.Errorf("failed to write manifest offset header: %w", err)
	}

	if _, err := file.Write(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write manifest content: %w", err)
	}

	logger.Tracef("Successfully wrote manifest %s with %d chunks.", manifestID, len(manifestList))
	return uniqueDoidList, nil
}

// readManifestFromFile reads and deserializes a manifest from a file.
func (x *XlatorDedup) readManifestFromFile(manifestID string) ([]ChunkInManifest, error) {
	path, err := x.getManifestPath(manifestID)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest path: %w", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	if len(data) < 8 {
		return nil, fmt.Errorf("manifest file %s is too small", path)
	}

	dobjIdOffset := internal.BytesToUInt64LittleEndian([8]byte(data[:8]))
	chunkData := data[8 : 8+dobjIdOffset]
	const chunkEntrySize = 32 + 8 + 8 // FP + Len + DOid
	if len(chunkData)%chunkEntrySize != 0 {
		return nil, fmt.Errorf("corrupted manifest chunk data section in %s", path)
	}

	numChunks := len(chunkData) / chunkEntrySize
	manifestList := make([]ChunkInManifest, numChunks)

	for i := 0; i < numChunks; i++ {
		offset := i * chunkEntrySize
		manifestList[i].FP = string(chunkData[offset : offset+32])
		manifestList[i].Len = binary.LittleEndian.Uint64(chunkData[offset+32 : offset+40])
		manifestList[i].DOid = binary.LittleEndian.Uint64(chunkData[offset+40 : offset+48])
	}

	logger.Tracef("Successfully read manifest %s with %d chunks.", manifestID, len(manifestList))
	return manifestList, nil
}

// deleteManifestFile removes a manifest file from the cache.
func (x *XlatorDedup) deleteManifestFile(manifestID string) error {
	if manifestID == "" {
		return nil // Nothing to delete
	}
	path, err := x.getManifestPath(manifestID)
	if err != nil {
		return fmt.Errorf("failed to get manifest path for deletion: %w", err)
	}

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		logger.Warnf("Failed to delete manifest file %s: %v", path, err)
		return err
	}
	logger.Tracef("Successfully deleted manifest file %s.", path)
	return nil
}

// readUniqueDoidsFromFile reads only the unique DOID set from a manifest file.
// This is more efficient than readManifestFromFile when only the DOID list is needed,
// for example during garbage collection reference counting.
func (x *XlatorDedup) readUniqueDoidsFromFile(manifestID string) ([]uint64, error) {
	path, err := x.getManifestPath(manifestID)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest path: %w", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest file: %w", err)
	}
	defer file.Close()

	// Read the 8-byte header to find the offset of the DOID set.
	var offsetHeader [8]byte
	if _, err := io.ReadFull(file, offsetHeader[:]); err != nil {
		return nil, fmt.Errorf("failed to read manifest offset header: %w", err)
	}
	dobjIdOffset := internal.BytesToUInt64LittleEndian(offsetHeader)

	// Seek directly to the start of the DOID set.
	if _, err = file.Seek(int64(8+dobjIdOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to DOID set in manifest file: %w", err)
	}

	// Read the rest of the file, which is the DOID set.
	doidSetData, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read DOID set from manifest file: %w", err)
	}

	if len(doidSetData)%8 != 0 {
		return nil, fmt.Errorf("corrupted manifest DOID set section in %s", path)
	}

	numDoids := len(doidSetData) / 8
	uniqueDoids := make([]uint64, numDoids)
	for i := 0; i < numDoids; i++ {
		offset := i * 8
		uniqueDoids[i] = binary.LittleEndian.Uint64(doidSetData[offset : offset+8])
	}

	logger.Tracef("Successfully read %d unique DOIDs from manifest %s.", len(uniqueDoids), manifestID)
	return uniqueDoids, nil
}
