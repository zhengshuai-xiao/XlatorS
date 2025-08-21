package dedup

import (
	"crypto/sha256"

	"github.com/zhengshuai-xiao/S3Store/internal"
)

func CalcFP(buf []byte, c *Chunk) {
	hash := sha256.New()
	hash.Write(buf[c.off : c.off+c.Len])
	c.FP = string(hash.Sum(nil))
	//c.FP = hex.EncodeToString(hashByte)
	logger.Tracef("CalcFP:fp:%s", internal.StringToHex(c.FP))
	//c.FP = hash.Sum(buf[c.off : c.off+c.len])
}

func CalcFPs(buf []byte, chunks []Chunk) {
	for i := range chunks {
		CalcFP(buf[chunks[i].off:chunks[i].off+chunks[i].Len], &chunks[i])
	}
}
