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

func (m *MockMDS) GetIncreasedDCID() (int64, error) {
	args := m.Called()
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

func (m *MockMDS) RemoveFPs(ns string, fps []string, dcid uint64) error {
	args := m.Called(ns, fps, dcid)
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

func (m *MockMDS) PutObjectMeta(objInfo minio.ObjectInfo, uniqueDCIDlist []uint64) error {
	args := m.Called(objInfo, uniqueDCIDlist)
	return args.Error(0)
}

func (m *MockMDS) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	args := m.Called(bucket, object)
	return args.Get(0).(minio.ObjectInfo), args.Error(1)
}

func (m *MockMDS) GetObjectMeta(object *minio.ObjectInfo) error {
	args := m.Called(object)
	return args.Error(0)
}

func (m *MockMDS) DelObjectMeta(bucket, object string) error {
	args := m.Called(bucket, object)
	return args.Error(0)
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

func (m *MockMDS) AddReference(namespace string, dataContainerIDs []uint64, objectName string) error {
	args := m.Called(namespace, dataContainerIDs, objectName)
	return args.Error(0)
}

func (m *MockMDS) RemoveReference(namespace string, dataContainerIDs []uint64, objectName string) ([]uint64, error) {
	args := m.Called(namespace, dataContainerIDs, objectName)
	return args.Get(0).([]uint64), args.Error(1)
}

func (m *MockMDS) IsDCIDDeleted(namespace string, dcid uint64) (bool, error) {
	args := m.Called(namespace, dcid)
	return args.Bool(0), args.Error(1)
}

func (m *MockMDS) NewRedisLock(bucket string, objects ...string) minio.RWLocker {
	args := m.Called(bucket, objects)
	return args.Get(0).(minio.RWLocker)
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
func (m *MockMDS) GetRandomDeletedDCIDs(namespace string, count int64) ([]uint64, error) {
	args := m.Called(namespace, count)
	var r0 []uint64
	if args.Get(0) != nil {
		r0 = args.Get(0).([]uint64)
	}
	return r0, args.Error(1)
}
func (m *MockMDS) RemoveSpecificDeletedDCIDs(namespace string, dcids []uint64) error {
	args := m.Called(namespace, dcids)
	return args.Error(0)
}
func (m *MockMDS) AddDeletedDCIDs(namespace string, dcids []uint64) error {
	args := m.Called(namespace, dcids)
	return args.Error(0)
}
