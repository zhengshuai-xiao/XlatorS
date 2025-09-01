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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	S3client "github.com/zhengshuai-xiao/XlatorS/pkg/s3client"
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

// writeManifest serializes a manifest, writes it to a local file, and conditionally uploads it to the S3 backend.
// The file format is:
// [8-byte offset to DOID set] [ChunkInManifest entries...] [Unique DOID set...]
func (x *XlatorDedup) writeManifest(ctx context.Context, namespace string, manifestID string, manifestList []ChunkInManifest) ([]uint64, error) {
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

	// Conditionally upload manifest file to S3 backend.
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return nil, fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		backendBucket := GetBackendBucketName(namespace)
		_, err = S3client.UploadFile(ctx, x.Client, backendBucket, manifestID, path)
		if err != nil {
			return nil, fmt.Errorf("failed to upload manifest file '%s' to S3 backend: %w", path, err)
		}
		logger.Tracef("Successfully uploaded manifest %s to S3 backend bucket %s.", manifestID, backendBucket)
	}

	return uniqueDoidList, nil
}

// readManifest reads and deserializes a manifest. It ensures the manifest is available locally, downloading from S3 if necessary.
func (x *XlatorDedup) readManifest(ctx context.Context, namespace, manifestID string) ([]ChunkInManifest, error) {
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

// deleteManifest removes a manifest from the local cache and the S3 backend.
func (x *XlatorDedup) deleteManifest(ctx context.Context, namespace, manifestID string) error {
	if manifestID == "" {
		return nil // Nothing to delete
	}

	// Delete from S3 backend first.
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		backendBucket := GetBackendBucketName(namespace)
		err := x.Client.RemoveObject(ctx, backendBucket, manifestID, miniogo.RemoveObjectOptions{})
		if err != nil && miniogo.ToErrorResponse(err).Code != "NoSuchKey" {
			logger.Warnf("Failed to delete manifest %s from S3 backend: %v", manifestID, err)
			// We can choose to continue to delete the local file anyway.
		}
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

// readUniqueDoids reads only the unique DOID set from a manifest.
// This is more efficient than readManifest when only the DOID list is needed,
// for example during garbage collection reference counting.
func (x *XlatorDedup) readUniqueDoids(ctx context.Context, namespace, manifestID string) ([]uint64, error) {
	path, err := x.ensureManifestLocal(ctx, namespace, manifestID)
	if err != nil {
		return nil, err
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

// ensureManifestLocal checks if a manifest file exists locally, and if not,
// downloads it from the S3 backend if configured.
func (x *XlatorDedup) ensureManifestLocal(ctx context.Context, namespace, manifestID string) (string, error) {
	path, err := x.getManifestPath(manifestID)
	if err != nil {
		return "", fmt.Errorf("failed to get manifest path: %w", err)
	}

	// Check if the manifest file exists locally.
	_, err = os.Stat(path)
	if err == nil {
		return path, nil // File exists locally.
	}

	if !os.IsNotExist(err) {
		// Another error occurred while stating the file.
		return "", fmt.Errorf("failed to stat manifest file %s: %w", path, err)
	}

	// File does not exist locally.
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return "", fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		logger.Tracef("Manifest %s not found in local cache, downloading from S3 backend.", manifestID)

		backendBucket := GetBackendBucketName(namespace)
		opts := miniogo.GetObjectOptions{}
		manifestReader, _, _, err := x.Client.GetObject(ctx, backendBucket, manifestID, opts)
		if err != nil {
			return "", fmt.Errorf("failed to get manifest %s from backend: %w", manifestID, err)
		}

		// Ensure the local directory exists before writing the file.
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			return "", fmt.Errorf("failed to create manifest directory for download: %w", err)
		}

		_, err = internal.WriteReadCloserToFile(manifestReader, path)
		if err != nil {
			return "", fmt.Errorf("failed to write manifest %s to local disk: %w", manifestID, err)
		}
		return path, nil
	}

	// If backend is posix and file doesn't exist, it's a not found error.
	return "", os.ErrNotExist
}
