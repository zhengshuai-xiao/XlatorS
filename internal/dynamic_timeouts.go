/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package internal

import (
	"time"

	minio "github.com/minio/minio/cmd"
)

// DynamicTimeout is an alias for minio.DynamicTimeout to provide a single
// point of definition for timeouts within the project.
type DynamicTimeout = minio.DynamicTimeout

// NewDynamicTimeout returns a new dynamic timeout initialized with timeout value.
func NewDynamicTimeout(timeout, minimum time.Duration) *DynamicTimeout {
	return minio.NewDynamicTimeout(timeout, minimum)
}
