package internal

type FixedCDC struct {
	// Add any necessary fields here
	Chunksize int
}

// TODO check l == N*Chunksize
func (f *FixedCDC) Chunking(buf []byte, l int) (chunks []Chunk, err error) {
	// Implement the FPCalc logic here
	cur := 0
	for {
		if cur+f.Chunksize > l {
			chunks = append(chunks, Chunk{off: cur, len: l - cur, Deduped: false, DOid: 0})
			break
		}

		chunks = append(chunks, Chunk{off: cur, len: f.Chunksize, Deduped: false, DOid: 0})
		cur = cur + f.Chunksize
	}
	return
}
