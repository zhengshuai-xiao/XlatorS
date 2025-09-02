package dedup

import (
	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/XlatorS/pkg/base"
)

func (m *MDSRedis) NewRedisLock(bucket string, objects ...string) minio.RWLocker {
	return base.NewRedisLock(m.Rdb, bucket, objects...)
}
