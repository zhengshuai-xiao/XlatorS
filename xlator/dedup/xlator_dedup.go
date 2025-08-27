// Copyright 2025 Zhenghshuai Xiao
//TODO: Which License should I use???
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
// limitations under the License.

package dedup

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	miniogo "github.com/minio/minio-go/v7"
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
	Client     *miniogo.Core
	HTTPClient *http.Client
	Metrics    *minio.BackendMetrics
	Mdsclient  MDS
	stopGC     chan struct{}  // Channel to signal GC stop
	wg         sync.WaitGroup // WaitGroup for graceful shutdown
}

var logger = internal.GetLogger("XlatorDedup")

func NewXlatorDedup(gConf *internal.Config) (*XlatorDedup, error) {

	if gConf.Xlator != XlatorName {
		logger.Fatalf("Invalid Xlator configuration(%s!=%s)", gConf.Xlator, XlatorName)
		return nil, fmt.Errorf("Invalid Xlator configuration(%s!=%s)", gConf.Xlator, XlatorName)
	}

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
	s3 := &S3client.S3{Host: gConf.BackendAddr}
	logger.Info("NewS3Gateway Endpoint:", s3.Host)
	creds := auth.Credentials{}
	client, err := s3.NewGatewayLayer(creds, t)
	if err != nil {
		logger.Errorf("failed to create S3 client: %v", err)
		return nil, err
	}

	xlatorDedup := &XlatorDedup{
		Client: client,
		HTTPClient: &http.Client{
			Transport: t,
		},
		Metrics:   metrics,
		Mdsclient: redismeta,
		stopGC:    make(chan struct{}),
	}

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

/*
	func (x *XlatorDedup) CreateBackendBucket() (err error) {
		ctx := context.Background()
		var ok bool
		if ok, err = x.Client.BucketExists(ctx, BackendBucket); err != nil {
			return minio.ErrorRespToObjectError(err, BackendBucket)
		}
		if !ok {
			err = x.Client.MakeBucket(ctx, BackendBucket, miniogo.MakeBucketOptions{Region: "us-east-1", ObjectLocking: false})
			if err != nil {
				return minio.ErrorRespToObjectError(err, BackendBucket)
			}
			logger.Infof("Created backend bucket: %s", BackendBucket)
		}

		return nil
	}
*/
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
	host := x.Client.EndpointURL().Host
	if x.Client.EndpointURL().Port() == "" {
		host = x.Client.EndpointURL().Host + ":" + x.Client.EndpointURL().Scheme
	}
	si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, host)

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
	logger.Infof("%s: enter+++++", internal.GetCurrentFuncName())
	start1 := time.Now()
	if err = ValidateBucketNameFormat(bucket); err != nil {
		logger.Errorf("invalid bucket name format for %s: %v", bucket, err)
		return
	}
	exist, err := x.Mdsclient.BucketExist(bucket)
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(err, bucket)
	}
	if !exist {
		err = minio.BucketNotFound{Bucket: bucket}
		return
	}
	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("failed to parse Namespace from %s", bucket)
		return objInfo, err
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
		// The ETag is stored in UserTags.
		if oldObjInfo.UserTags == newEtag {
			logger.Infof("PutObject: ETag match for %s/%s. Skipping upload.", bucket, object)
			// The object is identical, return the existing object's info.
			return oldObjInfo, nil //fmt.Errorf("same object[%s] already exists. If you still want to upload this object, please use another object name", object)
		}
	}

	//check if

	manifestID, err := x.Mdsclient.GetIncreasedManifestID()
	if err != nil {
		logger.Errorf("failed to get increased manifest ID: %v", err)
		return
	}
	logger.Trace("PutObject 1")
	isDir := false
	var etag string
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
	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ETag:        etag,
		ModTime:     time.Now(),
		Size:        0,
		IsDir:       isDir,
		AccTime:     time.Now(),
		UserTags:    opts.UserDefined["etag"],
		UserDefined: minio.CleanMetadata(opts.UserDefined),
		IsLatest:    true,
	}
	if objInfo.UserDefined == nil {
		objInfo.UserDefined = make(map[string]string)
	}
	objInfo.UserDefined[ManifestIDKey] = manifestID
	logger.Info("PutObject 2")
	var manifestList []ChunkInManifest
	manifestList, err = x.writeObj(ctx, ns, r, &objInfo)
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
	logger.Infof("PutObject(%s) took %vs, throughPut: %.2f MB/s", object, elapsed.Seconds(), float64(objInfo.Size)/(1024*1024)/elapsed.Seconds())
	logger.Infof("%s: end-----", internal.GetCurrentFuncName())
	return objInfo, nil
}

func (x *XlatorDedup) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	return
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
