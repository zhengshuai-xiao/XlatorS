package dedup

type Chunk struct {
	FP      string
	off     uint64
	Len     uint64
	Deduped bool
	DOid    uint64
	//OffInDOid uint64
	//LenInDOid uint64
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

type CDC interface {
	Chunking(buf []byte) (chunks []Chunk, err error)
}
