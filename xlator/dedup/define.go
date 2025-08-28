package dedup

import minio "github.com/minio/minio/cmd"

type Chunk struct {
	FP      string
	off     uint64
	Len     uint64
	Deduped bool
	DOid    uint64
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
	DOid uint64
	//OffInDOid uint64
	//LenInDOid uint64

}

type FPValInMDS struct {
	DOid uint64
	//OffInDOid uint64
	//LenInDOid uint64
}
