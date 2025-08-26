package dedup

import (
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
)

// reservedKeywords holds a set of names that cannot be used as a namespace or bucket name
// to avoid conflicts with internal system keys. The check is case-insensitive.
var reservedKeywords = map[string]struct{}{
	"dataobj":        {},
	"fpcache":        {},
	"buckets":        {},
	"ref":            {},
	"deleteddoid":    {},
	"datamanifestid": {},
	"dataobjid":      {},
	"globalns":       {},
	"dedup":          {},
}

// ValidateBucketNameFormat checks if a bucket name conforms to the required format.
// The format must be 'namespace.bucketname', and both parts must be at least 3 characters long.
// It also performs standard S3 bucket name validation and checks against reserved keywords.
func ValidateBucketNameFormat(bucket string) error {
	// Custom format validation: ns.bucket
	parts := strings.SplitN(bucket, ".", 2)
	if len(parts) != 2 {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("bucket name must be in 'namespace.bucket' format")}
	}

	namespace := parts[0]
	bucketNameOnly := parts[1]

	// Check against reserved keywords (case-insensitive)
	if _, ok := reservedKeywords[strings.ToLower(namespace)]; ok {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("namespace part '%s' is a reserved keyword", namespace)}
	}
	if _, ok := reservedKeywords[strings.ToLower(bucketNameOnly)]; ok {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("bucket part '%s' is a reserved keyword", bucketNameOnly)}
	}

	// Length validation for namespace and bucket name
	if len(namespace) < 3 {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("namespace part '%s' must be at least 3 characters long", namespace)}
	}
	if len(bucketNameOnly) < 3 {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("bucket part '%s' must be at least 3 characters long", bucketNameOnly)}
	}

	// Check for lowercase requirement
	if bucket != strings.ToLower(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket, Err: fmt.Errorf("bucket name can only contain lowercase letters")}
	}

	// Standard S3 bucket name validation (strict)
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return minio.BucketNameInvalid{Bucket: bucket, Err: err}
	}

	return nil
}
