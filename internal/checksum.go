package internal

import "hash/crc32"

// CalculateCRC32 computes the CRC-32 checksum of the data using the IEEE polynomial,
// which is the most common CRC32 standard.
func CalculateCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func VerifyCRC32(data []byte, crc uint32) bool {
	return crc32.ChecksumIEEE(data) == crc
}
