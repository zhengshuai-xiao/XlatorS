package dedup

import (
	minio "github.com/minio/minio/cmd"
)

const (
	manifestKey = "DatamanifestID"
	doidkey     = "DataObjID" //data object
)

type MDS interface {
	// Name of database
	Name() string
	// Init is used to initialize a meta service.
	Init(format *Format, force bool) error
	// Shutdown close current database connections.
	Shutdown() error
	MakeBucket(bucket string) error
	DelBucket(bucket string) error
	ListBuckets() ([]minio.BucketInfo, error)
	ListObjects(bucket string, prefix string) ([]minio.ObjectInfo, error)
	DelObjectMeta(bucket string, obj string) ([]int64, error)
	PutObjectMeta(object minio.ObjectInfo, manifestList []ChunkInManifest) error
	GetObjectMeta(object *minio.ObjectInfo) error
	BucketExist(bucket string) (bool, error)
	GetIncreasedDOID() (int64, error)
	GetDObjNameInMDS(id int64) string
	GetDOIDFromDObjName(string) (int64, error)
	GetIncreasedManifestID() (string, error)
	WriteManifest(manifestid string, manifestList []ChunkInManifest) error
	GetManifest(manifestid string) (chunks []ChunkInManifest, err error)
	DedupFPs(chunks []Chunk) error
	DedupFPsBatch(chunks []Chunk) error
	InsertFPs(chunks []ChunkInManifest) error
	InsertFPsBatch(chunks []ChunkInManifest) error
}
