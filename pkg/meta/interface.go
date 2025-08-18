package meta

import "github.com/minio/minio-go/v7"

type Meta interface {
	CreateBucket(bucket string) error
	DelBucket(bucket string) error
	ListBuckets() ([]minio.BucketInfo, error)
	ListObjects(bucket string) ([]minio.ObjectInfo, error)
	CommitObject(object minio.ObjectInfo) error
}
