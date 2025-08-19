package dedup

type Chunk struct {
	off     uint64
	len     uint64
	FP      string
	Deduped bool
	DOid    uint64
}
type ChunkInManifest struct {
	FP   string
	Off  uint64
	Len  uint64
	DOid uint64
}

type CDC interface {
	Chunking(buf []byte) (chunks []Chunk, err error)
}
