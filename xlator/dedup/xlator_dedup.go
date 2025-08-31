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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/XlatorS/internal"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
	S3client "github.com/zhengshuai-xiao/XlatorS/pkg/s3client"
)

const (
	sep                 = "/"
	XlatorName          = "Dedup"
	DefaultNS           = "globalns"
	BackendBucketPrefix = "dedup."
	ManifestIDKey       = "Manifestid"
)

type XlatorDedup struct {
	minio.GatewayUnsupported
	Client               *miniogo.Core
	HTTPClient           *http.Client
	Metrics              *minio.BackendMetrics
	Mdsclient            MDS
	dobjCachePath        string                       // The local directory used to cache data objects.
	stopGC               chan struct{}                // Channel to signal GC stop
	wg                   sync.WaitGroup               // WaitGroup for graceful shutdown
	multiPartFPCaches    map[string]map[string]uint64 // In-memory FP cache for active multipart uploads. Key: uploadID
	multiPartFPCachesMux sync.Mutex                   // Mutex to protect multiPartFPCaches
	fastCDCMinSize       int
	fastCDCAvgSize       int
	fastCDCMaxSize       int
	dsBackendType        DObjBackendType // datastore bankend type: "posix" or "s3"
}

var logger = internal.GetLogger("XlatorDedup")

func NewXlatorDedup(gConf *internal.Config) (*XlatorDedup, error) {

	if gConf.Xlator != XlatorName {
		logger.Fatalf("Invalid Xlator configuration(%s!=%s)", gConf.Xlator, XlatorName)
		return nil, fmt.Errorf("Invalid Xlator configuration(%s!=%s)", gConf.Xlator, XlatorName)
	}

	//prepare to connect to backend s3
	metaconf := NewMetaConfig()
	redismeta, err := NewRedisMeta(gConf.MetaDriver, gConf.MetaAddr, metaconf)
	if err != nil {
		logger.Errorf("failed to create redis MDS: %v", err)
		return nil, err
	}
	metrics := minio.NewMetrics()
	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	xlatorDedup := &XlatorDedup{
		HTTPClient: &http.Client{
			Transport: t,
		},
		Metrics:           metrics,
		Mdsclient:         redismeta,
		dobjCachePath:     gConf.DownloadCache,
		stopGC:            make(chan struct{}),
		multiPartFPCaches: make(map[string]map[string]uint64),
	}

	// Configure DObj backend type from command-line argument.
	xlatorDedup.dsBackendType = DObjBackendType(strings.ToLower(gConf.DSBackendType))
	if xlatorDedup.dsBackendType == "" {
		xlatorDedup.dsBackendType = DObjBackendPOSIX // Default to posix
	}
	if xlatorDedup.dsBackendType != DObjBackendPOSIX && xlatorDedup.dsBackendType != DObjBackendS3 {
		return nil, fmt.Errorf("invalid DObj backend type value: %s. Must be '%s' or '%s'", gConf.DSBackendType, DObjBackendPOSIX, DObjBackendS3)
	}
	logger.Infof("Using DObj backend type: %s", xlatorDedup.dsBackendType)

	if xlatorDedup.dsBackendType == DObjBackendS3 {
		s3 := &S3client.S3{Host: gConf.BackendAddr}
		logger.Info("NewS3Gateway Endpoint:", s3.Host)
		creds := auth.Credentials{}
		client, err := s3.NewGatewayLayer(creds, t)
		if err != nil {
			logger.Errorf("failed to create S3 client: %v", err)
			return nil, err
		}
		xlatorDedup.Client = client
	}

	// Configure FastCDC parameters from environment variables, with sane defaults.
	// Example: export XL_DEDUP_FASTCDC_AVG_SIZE=262144
	var parseEnvInt = func(key string, defaultValue int) int {
		valStr := os.Getenv(key)
		if valStr == "" {
			return defaultValue
		}
		val, err := strconv.Atoi(valStr)
		if err != nil {
			logger.Warnf("Invalid value for %s: '%s'. Using default %d. Error: %v", key, valStr, defaultValue, err)
			return defaultValue
		}
		logger.Infof("Using custom FastCDC config from env: %s=%d", key, val)
		return val
	}

	xlatorDedup.fastCDCMinSize = parseEnvInt("XL_DEDUP_FASTCDC_MIN_SIZE", 64*1024)  // 64KiB
	xlatorDedup.fastCDCAvgSize = parseEnvInt("XL_DEDUP_FASTCDC_AVG_SIZE", 128*1024) // 128KiB
	xlatorDedup.fastCDCMaxSize = parseEnvInt("XL_DEDUP_FASTCDC_MAX_SIZE", 256*1024) // 256KiB

	// Validate the cache path. An empty path would cause silent failure.
	if xlatorDedup.dobjCachePath == "" {
		err := fmt.Errorf("data object cache path (downloadCache) cannot be empty")
		logger.Errorf("%v", err)
		return nil, err
	}

	// Ensure the local cache directory for data objects exists.
	if err := os.MkdirAll(xlatorDedup.dobjCachePath, 0750); err != nil {
		logger.Errorf("failed to create data object cache directory %s: %v", xlatorDedup.dobjCachePath, err)
		return nil, err
	}
	logger.Infof("Data object cache directory '%s' is ready.", xlatorDedup.dobjCachePath)

	/*err = xlatorDedup.CreateBackendBucket()
	if err != nil {
		logger.Errorf("failed to create backend bucket: %v", err)
		return xlatorDedup, err
	}*/
	// Start the background GC worker
	//TODO: only leader can launch GC, so need leader selection
	xlatorDedup.startGC()
	// Start the admin command listener
	xlatorDedup.listenForAdminCommands()
	return xlatorDedup, nil
}

func (x *XlatorDedup) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down Dedup xlator...")
	close(x.stopGC) // Signal GC to stop
	x.wg.Wait()     // Wait for GC to finish
	logger.Info("Dedup xlator shut down gracefully.")
	return nil
}

func (x *XlatorDedup) PutObjectMetadata(ctx context.Context, s string, s2 string, options minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (x *XlatorDedup) IsCompressionSupported() bool {
	return false
}

func (x *XlatorDedup) IsEncryptionSupported() bool {
	return minio.GlobalKMS != nil || minio.GlobalGatewaySSE.IsSet()
	//return false
}

// IsReady returns whether the layer is ready to take requests.
func (x *XlatorDedup) IsReady(_ context.Context) bool {
	return true
}

// StorageInfo is not relevant to S3 backend.
func (x *XlatorDedup) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = madmin.Gateway
	if x.dsBackendType == DObjBackendS3 && x.Client != nil {
		host := x.Client.EndpointURL().Host
		if x.Client.EndpointURL().Port() == "" {
			host = x.Client.EndpointURL().Host + ":" + x.Client.EndpointURL().Scheme
		}
		si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, host)
	}
	// For 'posix' backend, GatewayOnline remains false, which is acceptable.
	return si, nil
}

func (x *XlatorDedup) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	//bucket = x.Mdsclient.ConvertBucketName(bucket)
	//delete the bucket in Meta
	err := x.Mdsclient.DelBucket(bucket)
	if err != nil {
		logger.Errorf("S3SObjects::DeleteBucket: failed to delete bucket(%s): %s", bucket, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}

	//delete the bucket in backend
	/*err = x.Client.RemoveBucket(ctx, bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}*/
	return nil
}

func (x *XlatorDedup) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}

	// Validate bucket name format before proceeding.
	// This enforces 'namespace.bucket' format with length constraints.
	if err := ValidateBucketNameFormat(bucket); err != nil {
		logger.Errorf("invalid bucket name format for %s: %v", bucket, err)
		return err
	}

	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		return err
	}

	//Create backend bucket with location(Namespace)
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		backendBucket := GetBackendBucketName(ns)
		ok, err := x.Client.BucketExists(ctx, backendBucket)
		if err != nil {
			return minio.ErrorRespToObjectError(err, backendBucket)
		}
		if !ok {
			//the bucket is not existed
			logger.Infof("new backend bucket name: %s in location:%s", backendBucket, opts.Location)
			err = x.Client.MakeBucket(ctx, backendBucket, miniogo.MakeBucketOptions{Region: opts.Location})
			if err != nil {
				return minio.ErrorRespToObjectError(err, backendBucket)
			}
		}
	}

	return x.Mdsclient.MakeBucket(bucket)
}

func (x *XlatorDedup) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	//bucket = x.Mdsclient.ConvertBucketName(bucket)
	if err = ValidateBucketNameFormat(bucket); err != nil {
		return bi, err
	}
	exist, err := x.Mdsclient.BucketExist(bucket)
	if err != nil {
		return bi, minio.ErrorRespToObjectError(err, bucket)
	}
	if !exist {
		return bi, minio.BucketNotFound{Bucket: bucket}
	}
	buckets, err := x.Mdsclient.ListBuckets()
	//buckets, err := x.Client.ListBuckets(ctx)
	if err != nil {
		// Listbuckets may be disallowed, proceed to check if
		// bucket indeed exists, if yes return success.
		logger.Errorf("failed to listBuckets from MDS")
		return bi, err
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.Created,
		}, nil
	}

	return bi, minio.BucketNotFound{Bucket: bucket}
}

func (x *XlatorDedup) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	buckets, err := x.Mdsclient.ListBuckets()

	//buckets, err := x.Client.ListBuckets(ctx)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err)
	}

	b := make([]minio.BucketInfo, len(buckets))
	for i, bi := range buckets {
		b[i] = minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.Created,
		}
		logger.Infof("ListBuckets: found bucket %s", bi.Name)
	}

	return b, err
}

func (x *XlatorDedup) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	//logger.Infof("%s: enter+++++", internal.GetCurrentFuncName())
	start1 := time.Now()
	if err = ValidateBucketNameFormat(bucket); err != nil {
		logger.Errorf("invalid bucket name format for %s: %v", bucket, err)
		return
	}
	exist, err := x.Mdsclient.BucketExist(bucket)
	if err != nil {
		logger.Errorf("PutObject: failed to check bucket existence: %v", err)
		return objInfo, minio.ErrorRespToObjectError(err, bucket)
	}
	if !exist {
		err = fmt.Errorf("internal error: bucket %s not found", bucket)
		logger.Error(err)
		err = minio.BucketNotFound{Bucket: bucket}
		return
	}
	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("failed to parse Namespace from %s", bucket)
		return objInfo, err
	}
	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return objInfo, fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		backendBucket := GetBackendBucketName(ns)
		exists, err := x.Client.BucketExists(ctx, backendBucket)
		if err != nil {
			logger.Errorf("PutObject: failed to check backend bucket %s existence: %v", backendBucket, err)
			return objInfo, minio.ErrorRespToObjectError(err, backendBucket)
		}
		if !exists {
			err = fmt.Errorf("internal error: backend bucket %s not found", backendBucket)
			logger.Error(err)
			return objInfo, minio.ErrorRespToObjectError(err, bucket)
		}
	}

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if opts.UserDefined["etag"] == "" {
		logger.Tracef("there is no etag, so set it with r.MD5CurrentHexString()")
		opts.UserDefined["etag"] = r.MD5CurrentHexString()
	}

	// Check if an object with the same ETag already exists.
	newEtag := opts.UserDefined["etag"]
	oldObjInfo, err := x.Mdsclient.GetObjectInfo(bucket, object)
	if err == nil {
		// Object with same name exists, check ETag.
		// The ETag is stored in the ETag field.
		if oldObjInfo.ETag == newEtag {
			logger.Infof("PutObject: ETag match for %s/%s. Skipping upload.", bucket, object)
			// The object is identical, return the existing object's info.
			return oldObjInfo, nil //fmt.Errorf("same object[%s] already exists. If you still want to upload this object, please use another object name", object)
		}
	}

	manifestID, err := x.Mdsclient.GetIncreasedManifestID()
	if err != nil {
		logger.Errorf("failed to get increased manifest ID: %v", err)
		return
	}
	//logger.Trace("PutObject 1")
	isDir := false
	if strings.HasSuffix(object, sep) {
		//TODO: is it meaningful???
		isDir = true
		if r.Size() > 0 {
			err = minio.ObjectExistsAsDirectory{
				Bucket: bucket,
				Object: object,
				Err:    syscall.EEXIST,
			}
			return
		}
	}

	logger.Tracef("opts.UserDefined=%v", opts.UserDefined)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ETag:        opts.UserDefined["etag"],
		ModTime:     time.Now(),
		Size:        0,
		IsDir:       isDir,
		AccTime:     time.Now(),
		UserDefined: minio.CleanMetadata(opts.UserDefined),
		IsLatest:    true,
	}
	if objInfo.UserDefined == nil {
		objInfo.UserDefined = make(map[string]string)
	}
	objInfo.UserDefined[ManifestIDKey] = manifestID
	//logger.Info("PutObject 2")

	// To align with the multipart upload pattern, we manage the FP cache in a central map
	// for the duration of this single-shot upload operation, using the manifestID as the key.
	x.multiPartFPCachesMux.Lock()
	localFPCache := make(map[string]uint64)
	x.multiPartFPCaches[manifestID] = localFPCache
	x.multiPartFPCachesMux.Unlock()

	// Ensure the temporary cache is cleaned up when the function returns.
	defer func() {
		x.multiPartFPCachesMux.Lock()
		delete(x.multiPartFPCaches, manifestID)
		x.multiPartFPCachesMux.Unlock()
	}()

	var manifestList []ChunkInManifest
	manifestList, err = x.writeObj(ctx, ns, r, &objInfo, localFPCache)
	if err != nil {
		logger.Errorf("failed to write data to object %s: %v", object, err)
		return objInfo, err
	}
	elapsed1 := time.Since(start1)
	start2 := time.Now()
	logger.Infof("PutObject 3, manifestList=%d, elapsed1=%f", len(manifestList), elapsed1.Seconds())
	//commit meta data
	err = x.Mdsclient.PutObjectMeta(objInfo, manifestList)
	if err != nil {
		logger.Errorf("failed to commit metadata for object %s: %v", object, err)
		return objInfo, err
	}
	elapsed2 := time.Since(start2)
	logger.Infof("PutObject 4, manifestList=%d, elapsed2=%f", len(manifestList), elapsed2.Seconds())
	err = x.Mdsclient.InsertFPsBatch(ns, manifestList)
	if err != nil {
		logger.Errorf("failed to insert fingerprints for object %s: %v", object, err)
		return objInfo, err
	}

	elapsed := time.Since(start1)
	throughput := 0.0
	if elapsed.Seconds() > 0 {
		throughput = float64(objInfo.Size) / (1024 * 1024) / elapsed.Seconds()
	}
	logger.Infof("Successfully put object %s/%s, size: %d, wrote: %s, dedupRate: %s, ETag: %s, elapsed: %s, throughput: %.2f MB/s",
		bucket, object, objInfo.Size, objInfo.UserDefined["wroteSize"], objInfo.UserDefined["dedupRate"], objInfo.ETag, elapsed, throughput)
	return objInfo, nil
}

func (x *XlatorDedup) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	logger.Infof("%s: enter: bucket=%s, object=%s", internal.GetCurrentFuncName(), bucket, object)

	if err = ValidateBucketNameFormat(bucket); err != nil {
		logger.Errorf("invalid bucket name format for %s: %v", bucket, err)
		return "", err
	}
	exist, err := x.Mdsclient.BucketExist(bucket)
	if err != nil {
		return "", minio.ErrorRespToObjectError(err, bucket)
	}
	if !exist {
		return "", minio.BucketNotFound{Bucket: bucket}
	}

	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("NewMultipartUpload: failed to parse namespace from bucket %s: %v", bucket, err)
		return "", err
	}

	if x.dsBackendType == DObjBackendS3 {
		if x.Client == nil {
			return "", fmt.Errorf("S3 backend is configured, but S3 client is not initialized")
		}
		backendBucket := GetBackendBucketName(ns)
		exists, err := x.Client.BucketExists(ctx, backendBucket)
		if err != nil {
			logger.Errorf("NewMultipartUpload: failed to check backend bucket %s existence: %v", backendBucket, err)
			return "", minio.ErrorRespToObjectError(err, backendBucket)
		}
		if !exists {
			err = fmt.Errorf("internal error: backend bucket %s not found", backendBucket)
			logger.Error(err)
			return "", minio.ErrorRespToObjectError(err, bucket)
		}
	}
	// Following PutObject, we get a manifest ID first. This will also serve as the upload ID.
	uploadID, err = x.Mdsclient.GetIncreasedManifestID()
	if err != nil {
		logger.Errorf("failed to get a unique ID for multipart upload: %v", err)
		return "", err
	}

	// Create a per-upload FP cache for intra-object deduplication across parts.
	x.multiPartFPCachesMux.Lock()
	x.multiPartFPCaches[uploadID] = make(map[string]uint64)
	x.multiPartFPCachesMux.Unlock()

	// Create a temporary ObjectInfo to hold the state of the multipart upload.
	// This is similar to how PutObject prepares an ObjectInfo.
	// The final size and ETag will be set upon completion.
	objInfo := minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(), // Represents initiation time
		UserDefined: minio.CleanMetadata(opts.UserDefined),
	}
	if objInfo.UserDefined == nil {
		objInfo.UserDefined = make(map[string]string)
	}
	// We store the manifest ID (which is our upload ID) in the UserDefined map.
	// This is consistent with how PutObject handles its manifest and will be needed
	// by CompleteMultipartUpload.
	objInfo.UserDefined[ManifestIDKey] = uploadID

	// Now, we need to persist this temporary object info, keyed by the uploadID.
	err = x.Mdsclient.InitMultipartUpload(uploadID, objInfo)
	if err != nil {
		logger.Errorf("failed to initialize multipart upload in metadata store for %s/%s: %v", bucket, object, err)
		return "", err
	}

	logger.Infof("Started new multipart upload for %s/%s with upload ID %s", bucket, object, uploadID)
	return uploadID, nil
}

func (x *XlatorDedup) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {
	logger.Infof("%s: enter: bucket=%s, object=%s, uploadID=%s, partID=%d", internal.GetCurrentFuncName(), bucket, object, uploadID, partID)

	// 1. Get multipart upload info to verify the call and get namespace
	objInfo, err := x.Mdsclient.GetMultipartUploadInfo(uploadID)
	if err != nil {
		logger.Errorf("PutObjectPart: failed to get multipart upload info for uploadID %s: %v", uploadID, err)
		if err == redis.Nil {
			return pi, minio.InvalidUploadID{UploadID: uploadID}
		}
		return pi, err
	}

	// Verify that the request parameters match the initiated upload
	if objInfo.Bucket != bucket || objInfo.Name != object {
		logger.Errorf("PutObjectPart: request parameters mismatch. Expected %s/%s, got %s/%s", objInfo.Bucket, objInfo.Name, bucket, object)
		return pi, minio.InvalidUploadID{UploadID: uploadID}
	}

	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("PutObjectPart: failed to parse namespace from bucket %s: %v", bucket, err)
		return pi, err
	}

	// Retrieve the dedicated FP cache for this multipart upload.
	x.multiPartFPCachesMux.Lock()
	localFPCache, ok := x.multiPartFPCaches[uploadID]
	x.multiPartFPCachesMux.Unlock()
	if !ok {
		// This can happen if the server restarts. The upload is still valid in Redis, but the in-memory cache is gone.
		// For now, we treat this as an error to enforce consistency. A more robust solution might re-create the cache.
		logger.Errorf("PutObjectPart: FP cache for uploadID %s not found. The upload might be stale or the server restarted.", uploadID)
		return pi, minio.InvalidUploadID{UploadID: uploadID}
	}

	// 2. Process data stream: chunk, dedup, and write new data chunks
	partSize, writtenSize, manifestList, err := x.writePart(ctx, ns, r, objInfo, localFPCache)
	if err != nil {
		logger.Errorf("PutObjectPart: failed to process part data for uploadID %s, partID %d: %v", uploadID, partID, err)
		return pi, err
	}

	// 3. Store part metadata and manifest
	partInfoWithStats := PartInfoWithStats{
		PartInfo: minio.PartInfo{
			PartNumber:   partID,
			ETag:         r.MD5CurrentHexString(),
			Size:         partSize,
			LastModified: time.Now().UTC(),
		},
		WrittenSize: writtenSize,
	}

	err = x.Mdsclient.AddMultipartPart(uploadID, partID, partInfoWithStats, manifestList)
	if err != nil {
		logger.Errorf("PutObjectPart: failed to store part metadata for uploadID %s, partID %d: %v", uploadID, partID, err)
		return pi, err
	}

	logger.Infof("Successfully uploaded part %d for %s/%s (uploadID: %s), size: %d", partID, bucket, object, uploadID, partSize)
	return partInfoWithStats.PartInfo, nil
}

// computeCompleteMultipartUploadETag calculates the ETag for a completed multipart upload.
// The ETag is the MD5 hash of the concatenated binary MD5 hashes of each part,
// followed by a hyphen and the number of parts.
// e.g., "d41d8cd98f00b204e9800998ecf8427e-1"
func computeCompleteMultipartUploadETag(partETags [][]byte) string {
	var etagBytes []byte
	for _, partETag := range partETags {
		etagBytes = append(etagBytes, partETag...)
	}
	h := md5.New()
	h.Write(etagBytes)
	md5sum := h.Sum(nil)
	return fmt.Sprintf("%s-%d", hex.EncodeToString(md5sum), len(partETags))
}

func (x *XlatorDedup) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (oi minio.ObjectInfo, e error) {
	logger.Infof("%s: enter: bucket=%s, object=%s, uploadID=%s", internal.GetCurrentFuncName(), bucket, object, uploadID)

	// 1. Get multipart upload info
	objInfo, err := x.Mdsclient.GetMultipartUploadInfo(uploadID)
	if err != nil {
		logger.Errorf("CompleteMultipartUpload: failed to get multipart upload info for uploadID %s: %v", uploadID, err)
		if err == redis.Nil {
			return oi, minio.InvalidUploadID{UploadID: uploadID}
		}
		return oi, err
	}

	// 2. Get all stored parts info and manifests for this upload
	storedPartsInfo, storedPartsManifest, err := x.Mdsclient.ListMultipartParts(uploadID)
	if err != nil {
		logger.Errorf("CompleteMultipartUpload: failed to list parts for uploadID %s: %v", uploadID, err)
		return oi, err
	}

	// 3. Verify client-provided parts and assemble final manifest
	var finalManifest []ChunkInManifest
	var totalSize int64
	var totalWrittenSize int64
	var partETagBytes [][]byte

	if len(uploadedParts) > 10000 {
		return oi, fmt.Errorf("too many parts=%d uploaded, it is greater than 10000", len(uploadedParts))
	}

	for _, clientPart := range uploadedParts {
		partIDStr := strconv.Itoa(clientPart.PartNumber)

		storedPart, ok := storedPartsInfo[partIDStr]
		if !ok {
			logger.Errorf("CompleteMultipartUpload: part %d not found for uploadID %s", clientPart.PartNumber, uploadID)
			return oi, minio.InvalidPart{}
		}

		if storedPart.ETag != clientPart.ETag {
			logger.Errorf("CompleteMultipartUpload: ETag mismatch for part %d. Client: %s, Stored: %s", clientPart.PartNumber, clientPart.ETag, storedPart.ETag)
			return oi, minio.InvalidPart{}
		}

		partManifest, ok := storedPartsManifest[partIDStr]
		if !ok {
			logger.Errorf("CompleteMultipartUpload: manifest for part %d not found. Data inconsistency for uploadID %s.", clientPart.PartNumber, uploadID)
			return oi, fmt.Errorf("Internal Error: Part manifest(partIDStr=%s) of %s/%s missing.", partIDStr, bucket, object) //minio.NewGoError("Internal Error: Part manifest missing.", "InternalError", http.StatusInternalServerError)
		}

		finalManifest = append(finalManifest, partManifest...)
		totalSize += storedPart.Size
		totalWrittenSize += storedPart.WrittenSize

		etag, err := hex.DecodeString(strings.Trim(clientPart.ETag, `"`))
		if err != nil {
			logger.Errorf("CompleteMultipartUpload: could not decode etag for part %d: %v", clientPart.PartNumber, err)
			return oi, minio.InvalidPart{}
		}
		partETagBytes = append(partETagBytes, etag)
	}

	finalETag := computeCompleteMultipartUploadETag(partETagBytes)

	objInfo.Size = totalSize
	objInfo.ETag = finalETag
	objInfo.ModTime = time.Now().UTC()
	if totalSize > 0 {
		dedupRate := float64(totalSize-totalWrittenSize) / float64(totalSize)
		objInfo.UserDefined["wroteSize"] = fmt.Sprintf("%d", totalWrittenSize)
		objInfo.UserDefined["dedupRate"] = fmt.Sprintf("%.2f%%", dedupRate*100)
	} else {
		objInfo.UserDefined["wroteSize"] = "0"
		objInfo.UserDefined["dedupRate"] = "0.00%"
	}

	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		return oi, err
	}

	if err = x.Mdsclient.PutObjectMeta(objInfo, finalManifest); err != nil {
		logger.Errorf("CompleteMultipartUpload: failed to commit final metadata for %s/%s: %v", bucket, object, err)
		return oi, err
	}

	if err = x.Mdsclient.InsertFPsBatch(ns, finalManifest); err != nil {
		logger.Errorf("CompleteMultipartUpload: failed to insert final fingerprints for %s/%s: %v", bucket, object, err)
		return oi, err
	}

	// Always remove the in-memory FP cache for this upload session.
	x.multiPartFPCachesMux.Lock()
	delete(x.multiPartFPCaches, uploadID)
	x.multiPartFPCachesMux.Unlock()

	if err = x.Mdsclient.CleanupMultipartUpload(uploadID); err != nil {
		logger.Warnf("CompleteMultipartUpload: failed to cleanup temporary data for uploadID %s: %v", uploadID, err)
	}

	logger.Infof("Successfully completed multipart upload for %s/%s, size: %d, wrote: %d, dedupRate: %s, ETag: %s",
		bucket, object, totalSize, totalWrittenSize, objInfo.UserDefined["dedupRate"], finalETag)
	return objInfo, nil
}

func (x *XlatorDedup) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	logger.Infof("%s: enter: bucket=%s, object=%s, uploadID=%s", internal.GetCurrentFuncName(), bucket, object, uploadID)

	// Verify the upload exists before cleaning up.
	_, err := x.Mdsclient.GetMultipartUploadInfo(uploadID)
	if err != nil {
		if err == redis.Nil {
			return minio.InvalidUploadID{UploadID: uploadID}
		}
		logger.Errorf("AbortMultipartUpload: failed to get multipart upload info for uploadID %s: %v", uploadID, err)
		return err
	}

	// Clean up metadata from Redis.
	if err := x.Mdsclient.CleanupMultipartUpload(uploadID); err != nil {
		logger.Warnf("AbortMultipartUpload: failed to cleanup temporary data for uploadID %s: %v", uploadID, err)
		// Continue to clean up in-memory cache anyway.
	}

	// Clean up the in-memory FP cache.
	x.multiPartFPCachesMux.Lock()
	delete(x.multiPartFPCaches, uploadID)
	x.multiPartFPCachesMux.Unlock()

	logger.Infof("Successfully aborted multipart upload for %s/%s with upload ID %s", bucket, object, uploadID)
	return nil
}

func (x *XlatorDedup) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (minio.MultipartInfo, error) {
	logger.Infof("%s: enter: bucket=%s, object=%s, uploadID=%s", internal.GetCurrentFuncName(), bucket, object, uploadID)

	objInfo, err := x.Mdsclient.GetMultipartUploadInfo(uploadID)
	if err != nil {
		if err == redis.Nil {
			return minio.MultipartInfo{}, minio.InvalidUploadID{UploadID: uploadID}
		}
		logger.Errorf("GetMultipartInfo: failed to get multipart upload info for uploadID %s: %v", uploadID, err)
		return minio.MultipartInfo{}, err
	}

	// Verify that the request parameters match the initiated upload
	if objInfo.Bucket != bucket || objInfo.Name != object {
		logger.Errorf("GetMultipartInfo: request parameters mismatch. Expected %s/%s, got %s/%s", objInfo.Bucket, objInfo.Name, bucket, object)
		return minio.MultipartInfo{}, minio.InvalidUploadID{UploadID: uploadID}
	}

	result := minio.MultipartInfo{
		Bucket:      objInfo.Bucket,
		Object:      objInfo.Name,
		UploadID:    uploadID,
		UserDefined: objInfo.UserDefined,
		Initiated:   objInfo.ModTime,
	}

	return result, nil
}

// TODO: add real clock
func (x *XlatorDedup) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	return &internal.StoreFLock{Owner: 123, Readonly: false}
}

// DeleteObject deletes a blob in bucket
func (x *XlatorDedup) DeleteObject(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())

	// Step 1: Delete metadata and get dereferenced DOIDs
	dobjIDs, err := x.Mdsclient.DelObjectMeta(bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	}

	// Step 2: If any DOIDs became dereferenced, add them to the GC queue
	if len(dobjIDs) > 0 {
		ns, _, err := ParseNamespaceAndBucket(bucket)
		if err != nil {
			logger.Errorf("DeleteObject: failed to parse namespace for bucket %s: %v", bucket, err)
			// Log the error but continue, as the primary object meta is already deleted.
		} else {
			err = x.Mdsclient.AddDeletedDOIDs(ns, dobjIDs)
			if err != nil {
				logger.Errorf("DeleteObject: failed to add DOIDs %v to GC queue for namespace %s: %v", dobjIDs, ns, err)
				// This is not a critical failure for the user, but should be monitored.
			} else {
				logger.Infof("DeleteObject: added %d DOIDs to GC queue for namespace %s.", len(dobjIDs), ns)
			}
		}
	}
	logger.Infof("DeleteObject: successfully deleted object %s/%s", bucket, object)
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

func (x *XlatorDedup) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = x.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return dobjects, errs
}

func (x *XlatorDedup) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucket, object)
	}
	err = x.readObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
	if err != nil {
		logger.Errorf("GetObject: failed to read object[%s] err: %s", object, err)
		return minio.ErrorRespToObjectError(err, bucket, object)
	}
	return nil
}

func (x *XlatorDedup) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	objInfo, err = x.Mdsclient.GetObjectInfo(bucket, object)
	if err != nil {
		logger.Errorf("GetObjectInfo:failed to GetObjectInfo[%s] err: %s", object, err)
		return objInfo, err
	}
	return
}

func (x *XlatorDedup) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	logger.Tracef("%s: enter:bucket=%s,object=%s", internal.GetCurrentFuncName(), bucket, object)
	var objInfo minio.ObjectInfo
	objInfo, err = x.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		logger.Errorf("GetObjectInfo:failed to GetObjectInfo[%s] err: %s", object, err)
		return nil, err
	}

	fn, off, length, err := minio.NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	pr, pw := io.Pipe()
	go func() {
		err := x.GetObject(ctx, bucket, object, off, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return fn(pr, h, opts.CheckPrecondFn, pipeCloser)
}

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (x *XlatorDedup) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	loi.IsTruncated = false
	loi.NextMarker = ""

	objects, err := x.Mdsclient.ListObjects(bucket, prefix)

	for _, obj := range objects {
		loi.Objects = append(loi.Objects, obj)
		logger.Tracef("ListObjects:add obj:%s", obj.Name)
	}
	if err != nil {
		return loi, err
	}
	return loi, nil
}
