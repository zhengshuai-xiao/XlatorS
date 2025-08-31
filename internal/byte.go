package internal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
)

// BytesToInt64LittleEndian 小端序字节数组转int64
func BytesToUInt64LittleEndian(b [8]byte) uint64 {
	// 先将字节数组解析为uint64，再转换为int64
	return uint64(binary.LittleEndian.Uint64(b[:]))
}
func UInt64ToBytesLittleEndian(i uint64) [8]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], i)
	return b
}

func SerializeToString(data interface{}) (string, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(data); err != nil {
		return "", fmt.Errorf("failed to serialize: %w", err)
	}
	return buf.String(), nil
}

func DeserializeFromString(data string, out interface{}) (err error) {
	buf := bytes.NewBufferString(data)
	decoder := gob.NewDecoder(buf)
	if err = decoder.Decode(out); err != nil {
		return fmt.Errorf("failed to deserialize: %w", err)
	}

	return nil
}

func StringToHex(s string) string {
	return hex.EncodeToString([]byte(s))
}

func HexToString(s string) (string, error) {
	data, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
