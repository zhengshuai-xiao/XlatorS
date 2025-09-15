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
)

// DataContainerBackend defines the interface for storing and retrieving data containers.
type DataContainerBackend interface {
	// Download retrieves an object from the backend and saves it to a local path.
	Download(ctx context.Context, bucket string, dcid uint64) (localPath string, err error)
	// GetUploader returns a writer to stream data to the backend.
	GetUploader(ctx context.Context, bucket string, dcid uint64) (uploader DataContainerUploader, localPath string, key string, err error)
	// Delete removes an object from the backend.
	Delete(ctx context.Context, bucket string, dcid uint64) error
}
