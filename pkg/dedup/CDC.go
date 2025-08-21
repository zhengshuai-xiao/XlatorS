package dedup

type Chunk struct {
	off       uint64
	Len       uint64
	Deduped   bool
	DOid      uint64
	OffInDOid uint64
	LenInDOid uint64
	FP        string
}
type ChunkInManifest struct {
	Len       uint64
	DOid      uint64
	OffInDOid uint64
	LenInDOid uint64
	FP        string
}

type FPValInMDS struct {
	DOid      uint64
	OffInDOid uint64
	LenInDOid uint64
}

type CDC interface {
	Chunking(buf []byte) (chunks []Chunk, err error)
}
