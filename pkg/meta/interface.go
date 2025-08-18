package meta

import minio "github.com/minio/minio/cmd"

type Meta interface {
	// Name of database
	Name() string
	// Init is used to initialize a meta service.
	Init(format *Format, force bool) error
	// Shutdown close current database connections.
	Shutdown() error
	MakeBucket(bucket string) error
	DelBucket(bucket string) error
	ListBuckets() ([]minio.BucketInfo, error)
	ListObjects(bucket string) ([]minio.ObjectInfo, error)
	PutObjectMeta(object minio.ObjectInfo) error
	GetObjectMeta(object minio.ObjectInfo) error
}
