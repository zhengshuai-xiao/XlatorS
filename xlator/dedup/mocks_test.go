package dedup

import (
	minio "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/mock"
)

// MockMDS is a mock implementation of the MDS interface for testing.
type MockMDS struct {
	mock.Mock
}

func (m *MockMDS) Name() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockMDS) Init(format *Format, force bool) error {
	args := m.Called(format, force)
	return args.Error(0)
}

func (m *MockMDS) Shutdown() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMDS) GetIncreasedDOID() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockMDS) GetDObjNameInMDS(doid uint64) string {
	args := m.Called(doid)
	return args.String(0)
}

func (m *MockMDS) GetDOIDFromDObjName(dobjName string) (int64, error) {
	args := m.Called(dobjName)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockMDS) DedupFPsBatch(ns string, chunks []Chunk) error {
	args := m.Called(ns, chunks)
	return args.Error(0)
}

func (m *MockMDS) DedupFPs(ns string, chunks []Chunk) error {
	args := m.Called(ns, chunks)
	return args.Error(0)
}

func (m *MockMDS) InsertFPsBatch(ns string, chunks []ChunkInManifest) error {
	args := m.Called(ns, chunks)
	return args.Error(0)
}

func (m *MockMDS) RemoveFPs(ns string, fps []string, doid uint64) error {
	args := m.Called(ns, fps, doid)
	return args.Error(0)
}

func (m *MockMDS) InsertFPs(ns string, chunks []ChunkInManifest) error {
	args := m.Called(ns, chunks)
	return args.Error(0)
}

func (m *MockMDS) MakeBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *MockMDS) BucketExist(bucket string) (bool, error) {
	args := m.Called(bucket)
	return args.Bool(0), args.Error(1)
}

func (m *MockMDS) DelBucket(bucket string) error {
	args := m.Called(bucket)
	return args.Error(0)
}

func (m *MockMDS) ListBuckets() ([]minio.BucketInfo, error) {
	args := m.Called()
	return args.Get(0).([]minio.BucketInfo), args.Error(1)
}

func (m *MockMDS) PutObjectMeta(objInfo minio.ObjectInfo, manifest []ChunkInManifest) error {
	args := m.Called(objInfo, manifest)
	return args.Error(0)
}

func (m *MockMDS) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	args := m.Called(bucket, object)
	return args.Get(0).(minio.ObjectInfo), args.Error(1)
}

func (m *MockMDS) GetObjectManifest(bucket, object string) ([]ChunkInManifest, error) {
	args := m.Called(bucket, object)
	return args.Get(0).([]ChunkInManifest), args.Error(1)
}

func (m *MockMDS) GetObjectMeta(object *minio.ObjectInfo) error {
	args := m.Called(object)
	return args.Error(0)
}

func (m *MockMDS) DelObjectMeta(bucket, object string) ([]uint64, error) {
	args := m.Called(bucket, object)
	return args.Get(0).([]uint64), args.Error(1)
}

func (m *MockMDS) ListObjects(bucket, prefix string) ([]minio.ObjectInfo, error) {
	args := m.Called(bucket, prefix)
	return args.Get(0).([]minio.ObjectInfo), args.Error(1)
}

func (m *MockMDS) GetIncreasedManifestID() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *MockMDS) InitMultipartUpload(uploadID string, objInfo minio.ObjectInfo) error {
	args := m.Called(uploadID, objInfo)
	return args.Error(0)
}

func (m *MockMDS) WriteManifest(manifestid string, manifestList []ChunkInManifest) error {
	args := m.Called(manifestid, manifestList)
	return args.Error(0)
}

func (m *MockMDS) GetMultipartUploadInfo(uploadID string) (minio.ObjectInfo, error) {
	args := m.Called(uploadID)
	return args.Get(0).(minio.ObjectInfo), args.Error(1)
}

func (m *MockMDS) AddMultipartPart(uploadID string, partID int, partInfo PartInfoWithStats, manifest []ChunkInManifest) error {
	args := m.Called(uploadID, partID, partInfo, manifest)
	return args.Error(0)
}

func (m *MockMDS) ListMultipartParts(uploadID string) (map[string]PartInfoWithStats, map[string][]ChunkInManifest, error) {
	args := m.Called(uploadID)
	return args.Get(0).(map[string]PartInfoWithStats), args.Get(1).(map[string][]ChunkInManifest), args.Error(2)
}

func (m *MockMDS) CleanupMultipartUpload(uploadID string) error {
	args := m.Called(uploadID)
	return args.Error(0)
}

func (m *MockMDS) GetManifest(manifestid string) ([]ChunkInManifest, error) {
	args := m.Called(manifestid)
	return args.Get(0).([]ChunkInManifest), args.Error(1)
}

func (m *MockMDS) AddReference(namespace string, dataObjectIDs []uint64, objectName string) error {
	args := m.Called(namespace, dataObjectIDs, objectName)
	return args.Error(0)
}

func (m *MockMDS) RemoveReference(namespace string, dataObjectIDs []uint64, objectName string) ([]uint64, error) {
	args := m.Called(namespace, dataObjectIDs, objectName)
	return args.Get(0).([]uint64), args.Error(1)
}

func (m *MockMDS) IsDOIDDeleted(namespace string, doid uint64) (bool, error) {
	args := m.Called(namespace, doid)
	return args.Bool(0), args.Error(1)
}

// Implement remaining MDS interface methods...
func (m *MockMDS) GetAllNamespaces() ([]string, error) {
	args := m.Called()
	var r0 []string
	if args.Get(0) != nil {
		r0 = args.Get(0).([]string)
	}
	return r0, args.Error(1)
}
func (m *MockMDS) GetRandomDeletedDOIDs(namespace string, count int64) ([]uint64, error) {
	args := m.Called(namespace, count)
	var r0 []uint64
	if args.Get(0) != nil {
		r0 = args.Get(0).([]uint64)
	}
	return r0, args.Error(1)
}
func (m *MockMDS) RemoveSpecificDeletedDOIDs(namespace string, doids []uint64) error {
	args := m.Called(namespace, doids)
	return args.Error(0)
}
func (m *MockMDS) AddDeletedDOIDs(namespace string, doids []uint64) error {
	args := m.Called(namespace, doids)
	return args.Error(0)
}
