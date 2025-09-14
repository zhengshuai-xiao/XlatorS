package main

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	M = 1 * 1024 * 1024
	G = 1 * 1024 * 1024 * 1024
)

// parseSize parses a string with a unit (M or G) and returns the size in bytes.
func parseSize(sizeStr string) (uint64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	lowerSizeStr := strings.ToLower(sizeStr)

	var multiplier uint64 = 1
	if strings.HasSuffix(lowerSizeStr, "g") {
		multiplier = G
		sizeStr = strings.TrimSuffix(lowerSizeStr, "g")
	} else if strings.HasSuffix(lowerSizeStr, "m") {
		multiplier = M
		sizeStr = strings.TrimSuffix(lowerSizeStr, "m")
	}

	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	return size * multiplier, nil
}
