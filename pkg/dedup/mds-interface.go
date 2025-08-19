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
	ListObjects(bucket string) ([]minio.ObjectInfo, error)
	PutObjectMeta(object minio.ObjectInfo) error
	GetObjectMeta(object minio.ObjectInfo) error
	BucketExist(bucket string) (bool, error)
	GetIncreasedDOID() (string, error)
	GetIncreasedManifestID() (string, error)
	WriteManifest(manifestid string, chunks []Chunk) error
	GetManifest(manifestid string) (chunks []ChunkInManifest, err error)
	DedupFPs(chunks []Chunk) error
	InsertFPs(chunks []ChunkInManifest) error
}
