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
	"os"
	"path/filepath"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

// S3Backend implements DataContainerBackend for an S3-compatible storage.
type S3Backend struct {
	dcCachePath string
	client      *miniogo.Core
}

// S3StreamUploader handles streaming a data container directly to S3.
type S3StreamUploader struct {
	pw      *io.PipeWriter
	errChan <-chan error
}

// GetWriter returns the pipe writer to stream data into.
func (u *S3StreamUploader) GetWriter() io.WriteCloser {
	return u.pw
}

// Wait blocks until the S3 upload is complete and returns the result.
func (u *S3StreamUploader) Wait() error {
	return <-u.errChan
}

func (s *S3Backend) getS3Key(dcid uint64) string {
	dcName := GetDCName(dcid)
	parentDirID := dcid / 1024
	return fmt.Sprintf("%d/%s", parentDirID, dcName)
}

// GetUploader for S3 backend sets up a pipe for streaming upload.
func (s *S3Backend) GetUploader(ctx context.Context, bucket string, dcid uint64) (DataContainerUploader, string, string, error) {
	key := s.getS3Key(dcid)
	pr, pw := io.Pipe()
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		opts := miniogo.PutObjectOptions{ContentType: "application/octet-stream"}
		_, err := s.client.PutObject(ctx, bucket, key, pr, -1, "", "", opts)
		if err != nil {
			pr.CloseWithError(err) // Ensure the reader side unblocks on error
		}
		errChan <- err
	}()

	return &S3StreamUploader{
		pw:      pw,
		errChan: errChan,
	}, "", key, nil // S3 backend doesn't produce a local path on upload
}

// Download retrieves the data container from the S3 backend if it's not present locally.
func (s *S3Backend) Download(ctx context.Context, bucket string, dcid uint64) (string, error) {
	key := s.getS3Key(dcid)
	dcName := GetDCName(dcid)
	parentDirID := dcid / 1024
	localPath := filepath.Join(s.dcCachePath, fmt.Sprintf("%d", parentDirID), dcName)

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create data container cache directory %s: %w", filepath.Dir(localPath), err)
	}

	dobjCloseReader, _, _, err := s.client.GetObject(ctx, bucket, key, miniogo.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get object %s from S3 backend: %w", key, err)
	}
	defer dobjCloseReader.Close()

	if _, err = internal.WriteReadCloserToFile(dobjCloseReader, localPath); err != nil {
		return "", fmt.Errorf("failed to write object %s to local disk: %w", key, err)
	}
	return localPath, nil
}

// Delete removes the object from the S3 backend and its corresponding local cache file.
func (s *S3Backend) Delete(ctx context.Context, bucket string, dcid uint64) error {
	key := s.getS3Key(dcid)

	// 1. Delete the object from the S3 backend.
	err := s.client.RemoveObject(ctx, bucket, key, miniogo.RemoveObjectOptions{})
	if err != nil && miniogo.ToErrorResponse(err).Code != "NoSuchKey" {
		// If it's any error other than "NoSuchKey", it's a failure.
		return err
	}

	// 2. Delete the corresponding file from the local cache, if it exists.
	dcName := GetDCName(dcid)
	parentDirID := dcid / 1024
	localPath := filepath.Join(s.dcCachePath, fmt.Sprintf("%d", parentDirID), dcName)
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		logger.Warnf("Failed to remove local cache file %s during S3 backend delete: %v", localPath, err)
	}

	return nil
}
