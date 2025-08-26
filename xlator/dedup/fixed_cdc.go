package dedup

type FixedCDC struct {
	// Add any necessary fields here
	Chunksize uint64
}

// TODO check l == N*Chunksize
func (f *FixedCDC) Chunking(buf []byte) (chunks []Chunk, err error) {
	// Implement the FPCalc logic here
	len := uint64(len(buf))
	cur := uint64(0)
	for {
		if cur+f.Chunksize > len {
			if len-cur != 0 {
				chunks = append(chunks, Chunk{off: cur, Len: len - cur, Deduped: false, DOid: 0})
			}
			break
		}

		chunks = append(chunks, Chunk{off: cur, Len: f.Chunksize, Deduped: false, DOid: 0})
		cur = cur + f.Chunksize
	}
	return
}
