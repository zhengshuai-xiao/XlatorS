package dedup

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/zhengshuai-xiao/S3Store/internal"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
	S3client "github.com/zhengshuai-xiao/S3Store/pkg/s3client"
)

const (
	sep           = "/"
	XlatorName    = "Dedup"
	BackendBucket = "xzstest"
)

type XlatorDedup struct {
	minio.GatewayUnsupported
	Client     *miniogo.Core
	HTTPClient *http.Client
	Metrics    *minio.BackendMetrics
	Mdsclient  MDS
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
	}

	err = xlatorDedup.CreateBackendBucket()
	if err != nil {
		logger.Errorf("failed to create backend bucket: %v", err)
		return xlatorDedup, err
	}
	return xlatorDedup, nil
}

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
func (x *XlatorDedup) Shutdown(ctx context.Context) error {
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
	//bucket := x.Mdsclient.ConvertBucketName(bucket_orig)
	logger.Infof("new bucket name: %s", bucket)
	// Verify if bucket name is valid.
	// We are using a separate helper function here to validate bucket
	// names instead of IsValidBucketName() because there is a possibility
	// that certains users might have buckets which are non-DNS compliant
	// in us-east-1 and we might severely restrict them by not allowing
	// access to these buckets.
	// Ref - http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if s3utils.CheckValidBucketName(bucket) != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	err := x.Mdsclient.MakeBucket(bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}

	/*err = x.Client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{Region: opts.Location})
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}*/

	return err
}
func (x *XlatorDedup) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	//bucket = x.Mdsclient.ConvertBucketName(bucket)
	err = x.isValidBucketName(bucket)
	if err != nil {
		logger.Errorf("bucket %s does not exist, err:%s", bucket, err)
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

func (x *XlatorDedup) isValidBucketName(bucket string) error {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	exist, err := x.Mdsclient.BucketExist(bucket)
	if !exist || err != nil {
		return minio.BucketNotFound{Bucket: bucket}
	}

	return nil
}

func (x *XlatorDedup) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	logger.Infof("%s: enter+++++", internal.GetCurrentFuncName())
	start1 := time.Now()
	err = x.isValidBucketName(bucket)
	if err != nil {
		logger.Errorf("this is a invalid bucket name [%s]", bucket)
		return
	}
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
		UserTags:    manifestID,
		UserDefined: minio.CleanMetadata(opts.UserDefined),
		IsLatest:    true,
	}
	//objInfo.UserDefined["BackendBucket"] = BackendBucket
	logger.Info("PutObject 2")
	var manifestList []ChunkInManifest
	manifestList, err = x.writeObj(ctx, r, &objInfo)
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
	err = x.Mdsclient.InsertFPsBatch(manifestList)
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

func (x *XlatorDedup) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	return &internal.StoreFLock{Owner: 123, Readonly: false}
}

// DeleteObject deletes a blob in bucket
func (x *XlatorDedup) DeleteObject(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	//delete in mds
	//delete manifest
	dobjIDs, err := x.Mdsclient.DelObjectMeta(bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	}
	//get unique DObjID from chunks
	//delete the reference of dobjIDs
	for _, objid := range dobjIDs {
		/*err = x.Client.RemoveObject(ctx, bucket, x.Mdsclient.GetDObjNameInMDS(objid), miniogo.RemoveObjectOptions{})
		if err != nil {
			return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
		}*/
		logger.Tracef("delete the reference of data object:%s", x.Mdsclient.GetDObjNameInMDS(objid))
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
	return
}

func (x *XlatorDedup) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	return
}

func (x *XlatorDedup) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	logger.Tracef("%s: enter", internal.GetCurrentFuncName())
	return nil, minio.NotImplemented{}
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
