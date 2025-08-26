package dedup

type CDC interface {
	Chunking(buf []byte) (chunks []Chunk, err error)
}
