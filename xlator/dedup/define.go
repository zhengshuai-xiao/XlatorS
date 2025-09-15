package dedup

import minio "github.com/minio/minio/cmd"

// DObjBackendType represents the type of backend storage for data objects.
type DObjBackendType string

const (
	// DObjBackendPOSIX uses a POSIX-compliant file system as the backend.
	DObjBackendPOSIX DObjBackendType = "posix"
	// DObjBackendS3 uses an S3-compatible object store as the backend.
	DObjBackendS3 DObjBackendType = "s3"
)

type Chunk struct {
	FP      string // Fingerprint of the chunk data
	Data    []byte // Chunk data, valid for the lifetime of the chunk
	Len     uint64
	Deduped bool
	DCID    uint64
	//OffInDOid uint64
	//LenInDOid uint64
}

type PartInfoWithStats struct {
	minio.PartInfo
	WrittenSize int64 `json:"writtenSize,omitempty"`
}
type ChunkInManifest struct {
	FP   string
	Len  uint64
	DCID uint64
	//OffInDOid uint64
	//LenInDOid uint64
}

type FPValInMDS struct {
	DCID uint64
	//OffInDOid uint64
	//LenInDOid uint64
}
