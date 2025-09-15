package dedup

import (
	minio "github.com/minio/minio/cmd"
)

const (
	manifestKey = "DatamanifestID"
	dcidkey     = "DataContainerID" //data container
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
	DelObjectMeta(bucket string, obj string) error
	PutObjectMeta(object minio.ObjectInfo, uniqueDCIDlist []uint64) error
	GetObjectMeta(object *minio.ObjectInfo) error
	GetObjectInfo(bucket string, obj string) (minio.ObjectInfo, error)
	BucketExist(bucket string) (bool, error)
	GetIncreasedDCID() (int64, error)
	GetIncreasedManifestID() (string, error)
	InitMultipartUpload(uploadID string, objInfo minio.ObjectInfo) error
	GetMultipartUploadInfo(uploadID string) (minio.ObjectInfo, error)
	AddMultipartPart(uploadID string, partID int, partInfo PartInfoWithStats, manifestList []ChunkInManifest) error
	ListMultipartParts(uploadID string) (map[string]PartInfoWithStats, map[string][]ChunkInManifest, error)
	CleanupMultipartUpload(uploadID string) error
	DedupFPs(namespace string, chunks []Chunk) error
	DedupFPsBatch(namespace string, chunks []Chunk) error
	InsertFPs(namespace string, chunks []ChunkInManifest) error
	InsertFPsBatch(namespace string, chunks []ChunkInManifest) error
	AddReference(namespace string, dataContainerIDs []uint64, objectName string) error
	RemoveReference(namespace string, dataContainerIDs []uint64, objectName string) (dereferencedDCIDs []uint64, err error)
	RemoveFPs(namespace string, FPs []string, DCID uint64) error
	AddDeletedDCIDs(namespace string, dcids []uint64) error
	GetRandomDeletedDCIDs(namespace string, count int64) ([]uint64, error)
	RemoveSpecificDeletedDCIDs(namespace string, dcids []uint64) error
	GetAllNamespaces() ([]string, error)
	IsDCIDDeleted(namespace string, dcid uint64) (bool, error)
	NewRedisLock(bucket string, objects ...string) minio.RWLocker
}
