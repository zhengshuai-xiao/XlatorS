package dedup

import (
	"fmt"
	"strconv"
	"strings"
)

type redisVersion struct {
	ver          string
	major, minor int
}

var oldestSupportedVer = redisVersion{"4.0.x", 4, 0}

func parseRedisVersion(v string) (ver redisVersion, err error) {
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		err = fmt.Errorf("invalid redisVersion: %v", v)
		return
	}
	ver.ver = v
	ver.major, err = strconv.Atoi(parts[0])
	if err != nil {
		return
	}
	ver.minor, err = strconv.Atoi(parts[1])
	return
}

func (ver redisVersion) olderThan(v2 redisVersion) bool {
	if ver.major < v2.major {
		return true
	}
	if ver.major > v2.major {
		return false
	}
	return ver.minor < v2.minor
}

func (ver redisVersion) String() string {
	return ver.ver
}

type redisInfo struct {
	aofEnabled      bool
	maxMemoryPolicy string
	redisVersion    string
	storageProvider string // redis is "", keyDB is "none" or "flash"
}

func checkRedisInfo(rawInfo string) (info redisInfo, err error) {
	lines := strings.Split(strings.TrimSpace(rawInfo), "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		kvPair := strings.SplitN(l, ":", 2)
		if len(kvPair) < 2 {
			continue
		}
		key, val := kvPair[0], kvPair[1]
		switch key {
		case "aof_enabled":
			info.aofEnabled = val == "1"
			if val == "0" {
				logger.Warnf("AOF is not enabled, you may lose data if Redis is not shutdown properly.")
			}
		case "maxmemory_policy":
			info.maxMemoryPolicy = val
		case "redis_version":
			info.redisVersion = val
			ver, err := parseRedisVersion(val)
			if err != nil {
				logger.Warnf("Failed to parse Redis server version %q: %s", ver, err)
			} else {
				if ver.olderThan(oldestSupportedVer) {
					logger.Fatalf("Redis version should not be older than %s", oldestSupportedVer)
				}
			}
		case "storage_provider":
			// if storage_provider is none reset it to ""
			if val == "flash" {
				info.storageProvider = val
			}
		}
	}
	return
}
