package dedup

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio/cmd"
)

type Config struct {
	Strict             bool // update ctime
	Retries            int
	MaxDeletes         int
	SkipDirNlink       int
	CaseInsensi        bool
	ReadOnly           bool
	NoBGJob            bool // disable background jobs like clean-up, backup, etc.
	OpenCache          time.Duration
	OpenCacheLimit     uint64 // max number of files to cache (soft limit)
	Heartbeat          time.Duration
	MountPoint         string
	Subdir             string
	AtimeMode          string
	DirStatFlushPeriod time.Duration
	SkipDirMtime       time.Duration
	Sid                uint64
	SortDir            bool
	FastStatfs         bool
}

func NewMetaConfig() *Config {
	return &Config{
		Strict:             true,
		Retries:            3,
		MaxDeletes:         100,
		SkipDirNlink:       1,
		CaseInsensi:        true,
		ReadOnly:           false,
		NoBGJob:            false,
		OpenCache:          5 * time.Minute,
		OpenCacheLimit:     100 * 1024 * 1024,
		Heartbeat:          30 * time.Second,
		MountPoint:         "/mnt",
		Subdir:             "subdir",
		AtimeMode:          "relatime",
		DirStatFlushPeriod: 5 * time.Second,
		SkipDirMtime:       1 * time.Second,
		Sid:                0,
		SortDir:            true,
		FastStatfs:         true,
	}
}

// redis setting
type Format struct {
	Name             string `json:"Name,omitempty"`
	UUID             string `json:"UUID,omitempty"`
	Storage          string `json:"Storage,omitempty"`
	StorageClass     string `json:"StorageClass,omitempty"`
	BucketPrefix     string `json:"BucketPrefix,omitempty"`
	AccessKey        string `json:"AccessKey,omitempty"`
	SecretKey        string `json:"SecretKey,omitempty"`
	SessionToken     string `json:"SessionToken,omitempty"`
	BlockSize        int    `json:"BlockSize,omitempty"`
	Compression      string `json:"Compression,omitempty"`
	Shards           int    `json:"Shards,omitempty"`
	HashPrefix       bool   `json:"HashPrefix,omitempty"`
	Capacity         uint64 `json:"Capacity,omitempty"`
	Inodes           uint64 `json:"Inodes,omitempty"`
	EncryptKey       string `json:"EncryptKey,omitempty"`
	EncryptAlgo      string `json:"EncryptAlgo,omitempty"`
	KeyEncrypted     bool   `json:"KeyEncrypted,omitempty"`
	UploadLimit      int64  `json:"UploadLimit,omitempty"`   // Mbps
	DownloadLimit    int64  `json:"DownloadLimit,omitempty"` // Mbps
	TrashDays        int    `json:"TrashDays,omitempty"`
	MetaVersion      int    `json:"MetaVersion,omitempty"`
	MinClientVersion string `json:"MinClientVersion,omitempty"`
	MaxClientVersion string `json:"MaxClientVersion,omitempty"`
	EnableACL        bool   `json:"EnableACL,omitempty"`
	RangerRestUrl    string `json:"RangerRestUrl,omitempty"`
	RangerService    string `json:"RangerService,omitempty"`
}

func (f *Format) update(old *Format, force bool) error {
	if force {
		logger.Warnf("Existing volume will be overwrited: %s", old)
	} else {
		var args []interface{}
		switch {
		case f.Name != old.Name:
			args = []interface{}{"name", old.Name, f.Name}
		case f.BlockSize != old.BlockSize:
			args = []interface{}{"block size", old.BlockSize, f.BlockSize}
		case f.Compression != old.Compression:
			args = []interface{}{"compression", old.Compression, f.Compression}
		case f.Shards != old.Shards:
			args = []interface{}{"shards", old.Shards, f.Shards}
		case f.HashPrefix != old.HashPrefix:
			args = []interface{}{"hash prefix", old.HashPrefix, f.HashPrefix}
		case f.MetaVersion != old.MetaVersion:
			args = []interface{}{"meta version", old.MetaVersion, f.MetaVersion}
		}
		if args == nil {
			f.UUID = old.UUID
		} else {
			return fmt.Errorf("cannot update volume %s from %v to %v", args...)
		}
	}
	return nil
}

type BucketInfo struct {
	Name             string    `json:"Name,omitempty"`
	UUID             string    `json:"UUID,omitempty"`
	Created          time.Time `json:"Created,omitempty"`
	BlockSize        int       `json:"BlockSize,omitempty"`
	ChunkMethod      string    `json:"ChunkMethod,omitempty"`
	Compression      string    `json:"Compression,omitempty"`
	Shards           int       `json:"Shards,omitempty"`
	HashPrefix       bool      `json:"HashPrefix,omitempty"`
	Capacity         uint64    `json:"Capacity,omitempty"`
	EncryptKey       string    `json:",omitempty"`
	EncryptAlgo      string    `json:",omitempty"`
	KeyEncrypted     bool      `json:",omitempty"`
	UploadLimit      int64     `json:",omitempty"`
	DownloadLimit    int64     `json:",omitempty"`
	TrashDays        int       `json:",omitempty"`
	MetaVersion      int       `json:",omitempty"`
	MinClientVersion string    `json:",omitempty"`
	MaxClientVersion string    `json:",omitempty"`
	EnableACL        bool      `json:",omitempty"`
}

func newBucketInfo(name string) *BucketInfo {
	bucketinfo := BucketInfo{
		Name:        name,
		UUID:        uuid.New().String(),
		Created:     time.Now(),
		BlockSize:   4 * 1024 * 1024,
		ChunkMethod: "fixed:128k",
		Shards:      1,
	}
	return &bucketinfo
}

type ObjectInfo struct {
	obj minio.ObjectInfo
}

/*type ObjectInfo struct {
	Name       string `json:"Name"`
	Size       int64  `json:"Size"`
	ETag       string `json:"ETag"`
	Bucket     string `json:"Bucket"`
	DataObject string `json:"DataObject"`
	Retention  string `json:"Retention"`
}*/
