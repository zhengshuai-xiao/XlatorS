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
)

// POSIXFileUploader handles writing a data container to a local file.
type POSIXFileUploader struct {
	filer *os.File
}

// GetWriter returns the file handle to write to.
func (p *POSIXFileUploader) GetWriter() io.WriteCloser {
	return p.filer
}

// Wait closes the file handle. For POSIX, the "upload" is complete once written.
func (p *POSIXFileUploader) Wait() error {
	return nil // For POSIX, the file is "uploaded" once closed. Nothing to wait for.
}

// Close closes the underlying file.
func (p *POSIXFileUploader) Close() error {
	return p.filer.Close()
}

// POSIXBackend implements DataContainerBackend for a local POSIX filesystem.
type POSIXBackend struct {
	dcCachePath string
}

func (p *POSIXBackend) getLocalPath(dcid uint64) string {
	dcName := GetDCName(dcid)
	parentDirID := dcid / 1024
	return filepath.Join(p.dcCachePath, fmt.Sprintf("%d", parentDirID), dcName)
}

// GetUploader for POSIX backend opens a local file for writing.
func (p *POSIXBackend) GetUploader(ctx context.Context, bucket string, dcid uint64) (DataContainerUploader, string, string, error) {
	localPath := p.getLocalPath(dcid)

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return nil, "", "", fmt.Errorf("failed to create data container parent directory %s: %w", filepath.Dir(localPath), err)
	}

	filer, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, "", "", err
	}
	return &POSIXFileUploader{
		filer: filer,
	}, localPath, filepath.Base(localPath), nil
}

// Download for POSIX backend just checks if the file exists, as it's expected to be local.
func (p *POSIXBackend) Download(ctx context.Context, bucket string, dcid uint64) (string, error) {
	localPath := p.getLocalPath(dcid)
	// For POSIX, if the file doesn't exist locally, it's a hard error.
	_, err := os.Stat(localPath)
	return localPath, err
}

// Delete removes the local data container file.
func (p *POSIXBackend) Delete(ctx context.Context, bucket string, dcid uint64) error {
	localPath := p.getLocalPath(dcid)
	return os.Remove(localPath)
}
