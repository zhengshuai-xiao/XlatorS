package gateway

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/madmin"
	"github.com/zhengshuai-xiao/S3Store/internal"
	s3meta "github.com/zhengshuai-xiao/S3Store/pkg/meta"

	minio "github.com/minio/minio/cmd"
)

const (
	sep           = "/"
	metaBucket    = ".sys"
	subDirPrefix  = 3 // 16^3=4096 slots
	nullVersionID = "null"
)

var logger = internal.GetLogger("juicefs")

type Config struct {
	MultiBucket bool
	KeepEtag    bool
	Umask       uint16
	ObjTag      bool
	ObjMeta     bool
	HeadDir     bool
	HideDir     bool
	ReadOnly    bool
	BackendAddr string
	Creds       *credentials.Credentials
	MetaDriver  string
	MetaAddr    string
}

func NewS3Gateway(gConf *Config) (minio.ObjectLayer, error) {
	//mctx = s3meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})

	s3 := &S3{host: gConf.BackendAddr}
	logger.Info("NewS3Gateway Endpoint:", s3.host)
	creds := auth.Credentials{}
	s3store := NewS3SObject(gConf)
	err := s3.NewGatewayLayer(creds, s3store)

	if err != nil {
		logger.Errorf("failed to create S3 client: %v", err)
		return nil, err
	}
	//s3store := &S3SObjects{"xzs!!!", s3client}
	return s3store, err
}

// s3Objects implements gateway for MinIO and S3 compatible object storage servers.
type S3SObjects struct {
	minio.GatewayUnsupported
	Client     *miniogo.Core
	HTTPClient *http.Client
	Metrics    *minio.BackendMetrics
	Metaclient s3meta.Meta
}

func NewS3SObject(gConf *Config) *S3SObjects {
	metaconf := s3meta.NewMetaConfig()
	redismeta, err := s3meta.NewRedisMeta(gConf.MetaDriver, gConf.MetaAddr, metaconf)
	if err != nil {
		logger.Errorf("failed to create redis s3meta: %v", err)
		return nil
	}
	return &S3SObjects{
		Client:     nil,
		HTTPClient: nil,
		Metrics:    nil,
		Metaclient: redismeta,
	}
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (n *S3SObjects) Shutdown(ctx context.Context) error {
	return nil
}

func (n *S3SObjects) PutObjectMetadata(ctx context.Context, s string, s2 string, options minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (n *S3SObjects) IsCompressionSupported() bool {
	return false
}

func (n *S3SObjects) IsEncryptionSupported() bool {
	return minio.GlobalKMS != nil || minio.GlobalGatewaySSE.IsSet()
	//return false
}

// IsReady returns whether the layer is ready to take requests.
func (n *S3SObjects) IsReady(_ context.Context) bool {
	return true
}

// StorageInfo is not relevant to S3 backend.
func (l *S3SObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = madmin.Gateway
	host := l.Client.EndpointURL().Host
	if l.Client.EndpointURL().Port() == "" {
		host = l.Client.EndpointURL().Host + ":" + l.Client.EndpointURL().Scheme
	}
	si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, host)

	return si, nil
}

func (n *S3SObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	//bucket = n.Metaclient.ConvertBucketName(bucket)
	//delete the bucket in Meta
	err := n.Metaclient.DelBucket(bucket)
	if err != nil {
		logger.Errorf("S3SObjects::DeleteBucket: failed to delete bucket(%s): %s", bucket, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}

	//delete the bucket in backend
	err = n.Client.RemoveBucket(ctx, bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return nil
}

func (n *S3SObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}
	//bucket := n.Metaclient.ConvertBucketName(bucket_orig)
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
	err := n.Metaclient.MakeBucket(bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}

	err = n.Client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{Region: opts.Location})
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}

	return err
}

func (n *S3SObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	//bucket = n.Metaclient.ConvertBucketName(bucket)

	buckets, err := n.Metaclient.ListBuckets()
	//buckets, err := n.Client.ListBuckets(ctx)
	if err != nil {
		// Listbuckets may be disallowed, proceed to check if
		// bucket indeed exists, if yes return success.
		var ok bool
		if ok, err = n.Client.BucketExists(ctx, bucket); err != nil {
			return bi, minio.ErrorRespToObjectError(err, bucket)
		}
		if !ok {
			return bi, minio.BucketNotFound{Bucket: bucket}
		}
		return minio.BucketInfo{
			Name:    bucket,
			Created: time.Now().UTC(),
		}, nil
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

func (n *S3SObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	buckets, err := n.Metaclient.ListBuckets()

	//buckets, err := n.Client.ListBuckets(ctx)
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

// ListObjects lists all blobs in JFS bucket filtered by prefix.
func (n *S3SObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {

	return loi, minio.NotImplemented{}
}

// ListObjectsV2 lists all blobs in JFS bucket filtered by prefix
func (n *S3SObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {

	return loi, minio.NotImplemented{}
}

func (n *S3SObjects) setFileAtime(p string, atime int64) {

}

func (n *S3SObjects) DeleteObject(ctx context.Context, bucket, object string, options minio.ObjectOptions) (info minio.ObjectInfo, err error) {

	return info, minio.NotImplemented{}
}

func (n *S3SObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, options minio.ObjectOptions) (objs []minio.DeletedObject, errs []error) {
	objs = make([]minio.DeletedObject, len(objects))
	errs = make([]error, len(objects))

	return objs, errs
}

func (n *S3SObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	return nil, minio.NotImplemented{}
}

func (n *S3SObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	return
}

var buffPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 1<<17)
		return &buf
	},
}

func (n *S3SObjects) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	return
}

func (n *S3SObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return
}

func (n *S3SObjects) putObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions, applyObjTaggingFunc func(tmpName string)) (err error) {
	return
}

func (n *S3SObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return
}

func (n *S3SObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	return
}

const uploadKeyName = "s3-object"
const s3Etag = "s3-etag"

// less than 64k ref: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions
const s3Tags = "s3-tags"

// S3 object metadata
const s3Meta = "s3-meta"
const amzMeta = "x-amz-meta-"

var s3UserControlledSystemMeta = []string{
	"cache-control",
	"content-disposition",
	"content-type",
}

func (n *S3SObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	return
}

func (n *S3SObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (result minio.PartInfo, err error) {
	return
}

func (n *S3SObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	return
}

func (n *S3SObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	return
}

func (n *S3SObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return
}

func (n *S3SObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, option minio.ObjectOptions) (err error) {
	return
}

// TODO: finish it
func (n *S3SObjects) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	return &internal.StoreFLock{Owner: 123, Readonly: false}
}

func (n *S3SObjects) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.FS}
}

func (n *S3SObjects) LocalStorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return n.StorageInfo(ctx)
}

func (n *S3SObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi minio.ListObjectVersionsInfo, err error) {
	return
}

func (n *S3SObjects) getObjectInfoNoFSLock(ctx context.Context, bucket, object string, info *minio.ObjectInfo) (oi minio.ObjectInfo, e error) {
	return n.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
}

func (n *S3SObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- minio.ObjectInfo, opts minio.ObjectOptions) error {
	//return minio.FSWalk(ctx, n, bucket, prefix, n.listDirFactory(), n.isLeaf, n.isLeafDir, results, n.getObjectInfoNoFSLock, n.getObjectInfoNoFSLock)
	return nil
}

func (n *S3SObjects) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return minio.NotImplemented{}
}

func (n *S3SObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return nil, minio.NotImplemented{}
}

func (n *S3SObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return minio.NotImplemented{}
}

func (n *S3SObjects) SetDriveCounts() []int {
	return nil
}

func (n *S3SObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

func (n *S3SObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

func (n *S3SObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (res madmin.HealResultItem, err error) {
	return res, minio.NotImplemented{}
}

func (n *S3SObjects) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn minio.HealObjectFn) error {
	return minio.NotImplemented{}
}

func (n *S3SObjects) GetMetrics(ctx context.Context) (*minio.BackendMetrics, error) {
	return &minio.BackendMetrics{}, minio.NotImplemented{}
}

func (n *S3SObjects) Health(ctx context.Context, opts minio.HealthOptions) minio.HealthResult {
	return minio.HealthResult{
		Healthy: true,
	}
}

func (n *S3SObjects) ReadHealth(ctx context.Context) bool {
	return false
}

func (n *S3SObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return n.GetObjectInfo(ctx, bucket, object, opts)
}

func (fs *S3SObjects) GetObjectTags(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (*tags.Tags, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, minio.VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	oi, err := fs.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

func (n *S3SObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {

	return minio.ObjectInfo{}, minio.NotImplemented{}

}

func (n *S3SObjects) IsNotificationSupported() bool {
	return true
}

func (n *S3SObjects) IsListenSupported() bool {
	return true
}

func (n *S3SObjects) IsTaggingSupported() bool {
	return true
}

func (n *S3SObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	return
}

/*
// CrawlAndGetDataUsage returns data usage stats of the current FS deployment
func (fs *S3SObjects) CrawlAndGetDataUsage(ctx context.Context, bf *minio.BloomFilter, updates chan<- minio.DataUsageInfo) error {
	return nil
}*/

func (n *S3SObjects) NSScanner(ctx context.Context, bf *minio.BloomFilter, updates chan<- madmin.DataUsageInfo) error {
	return nil
}
