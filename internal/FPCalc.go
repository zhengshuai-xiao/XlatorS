package internal

import (
	"crypto/sha256"
	"encoding/hex"
)

func CalcFP(buf []byte, c *Chunk) {
	hash := sha256.New()
	c.FP = hex.EncodeToString(hash.Sum(buf[c.off : c.off+c.len]))
	//c.FP = hash.Sum(buf[c.off : c.off+c.len])
}

func CalcFPs(buf []byte, chunks []Chunk) {
	for i := range chunks {
		CalcFP(buf[chunks[i].off:chunks[i].off+chunks[i].len], &chunks[i])
	}
}
