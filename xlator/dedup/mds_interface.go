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
	DelObjectMeta(bucket string, obj string) ([]uint64, error)
	PutObjectMeta(object minio.ObjectInfo, manifestList []ChunkInManifest) error
	GetObjectMeta(object *minio.ObjectInfo) error
	GetObjectInfo(bucket string, obj string) (minio.ObjectInfo, error)
	BucketExist(bucket string) (bool, error)
	GetIncreasedDOID() (int64, error)
	GetDObjNameInMDS(id uint64) string
	GetDOIDFromDObjName(string) (int64, error)
	GetIncreasedManifestID() (string, error)
	WriteManifest(manifestid string, manifestList []ChunkInManifest) error
	InitMultipartUpload(uploadID string, objInfo minio.ObjectInfo) error
	GetMultipartUploadInfo(uploadID string) (minio.ObjectInfo, error)
	AddMultipartPart(uploadID string, partID int, partInfo PartInfoWithStats, manifestList []ChunkInManifest) error
	ListMultipartParts(uploadID string) (map[string]PartInfoWithStats, map[string][]ChunkInManifest, error)
	CleanupMultipartUpload(uploadID string) error
	GetManifest(manifestid string) (chunks []ChunkInManifest, err error)
	GetObjectManifest(bucket, object string) (chunks []ChunkInManifest, err error)
	DedupFPs(namespace string, chunks []Chunk) error
	DedupFPsBatch(namespace string, chunks []Chunk) error
	InsertFPs(namespace string, chunks []ChunkInManifest) error
	InsertFPsBatch(namespace string, chunks []ChunkInManifest) error
	AddReference(namespace string, dataObjectIDs []uint64, objectName string) error
	RemoveReference(namespace string, dataObjectIDs []uint64, objectName string) (dereferencedDObjIDs []uint64, err error)
	RemoveFPs(namespace string, FPs []string, DOid uint64) error
	AddDeletedDOIDs(namespace string, doids []uint64) error
	GetRandomDeletedDOIDs(namespace string, count int64) ([]uint64, error)
	RemoveSpecificDeletedDOIDs(namespace string, doids []uint64) error
	GetAllNamespaces() ([]string, error)
	IsDOIDDeleted(namespace string, doid uint64) (bool, error)
}
