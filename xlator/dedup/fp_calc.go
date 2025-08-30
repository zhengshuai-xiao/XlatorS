package dedup

import (
	"crypto/sha256"

	"github.com/zhengshuai-xiao/XlatorS/internal"
)

func CalcFP(buf []byte, c *Chunk) {
	hash := sha256.New()
	hash.Write(buf)
	c.FP = string(hash.Sum(nil))
	//c.FP = hex.EncodeToString(hashByte)
	logger.Tracef("CalcFP:fp:%s", internal.StringToHex(c.FP))
	//c.FP = hash.Sum(buf[c.off : c.off+c.len])
}

func CalcFPs(chunks []Chunk) {
	for i := range chunks {
		CalcFP(chunks[i].Data, &chunks[i])
	}
}
