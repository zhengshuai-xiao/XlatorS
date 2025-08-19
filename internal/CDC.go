package internal

type Chunk struct {
	off     int
	len     int
	FP      string
	Deduped bool
	DOid    int64
}
type ChunkinManifest struct {
	FP   string
	len  int
	DOid int64
}

type CDC interface {
	Chunking(buf []byte, l int) (chunks []Chunk, err error)
}
