package dedup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio/cmd"
	redis "github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/S3Store/internal"
)

/*
Node:       i$inode -> Attribute{type,mode,uid,gid,atime,mtime,ctime,nlink,length,rdev}
Dir:        d$inode -> {name -> {inode,type}}
Parent:     p$inode -> {parent -> count} // for hard links
File:       c$inode_$indx -> [Slice{pos,id,length,off,len}]
Symlink:    s$inode -> target
Xattr:      x$inode -> {name -> value}
Flock:      lockf$inode -> { $sid_$owner -> ltype }
POSIX lock: lockp$inode -> { $sid_$owner -> Plock(pid,ltype,start,end) }
Sessions:   sessions -> [ $sid -> heartbeat ]
sustained:  session$sid -> [$inode]
locked:     locked$sid -> { lockf$inode or lockp$inode }

Removed files: delfiles -> [$inode:$length -> seconds]
detached nodes: detachedNodes -> [$inode -> seconds]
Slices refs: sliceRef -> {k$sliceId_$size -> refcount}

Dir data length:   dirDataLength -> { $inode -> length }
Dir used space:    dirUsedSpace -> { $inode -> usedSpace }
Dir used inodes:   dirUsedInodes -> { $inode -> usedInodes }
Quota:             dirQuota -> { $inode -> {maxSpace, maxInodes} }
Quota used space:  dirQuotaUsedSpace -> { $inode -> usedSpace }
Quota used inodes: dirQuotaUsedInodes -> { $inode -> usedInodes }
Acl: acl -> { $acl_id -> acl }

Redis features:

	Sorted Set: 1.2+
	Hash Set: 4.0+
	Transaction: 2.2+
	Scripting: 2.6+
	Scan: 2.8+
*/
const (
	KeyExists  = 1
	DOKeyWord  = "DataObj"
	FPCacheKey = "FPCache"
	BucketsKey = "Buckets"
	ObjectFake = "ObjectFake"
)

type MDSRedis struct {
	//*baseMeta
	rdb          redis.UniversalClient
	prefix       string //DB name
	bucketPrefix string //BUK
	ObjectPrefic string //OBJ
	shaLookup    string // The SHA returned by Redis for the loaded `scriptLookup`
	shaResolve   string // The SHA returned by Redis for the loaded `scriptResolve`
	metesetting  Format
}

//var _ Meta = (*MDSRedis)(nil)
//var _ engine = (*MDSRedis)(nil)

// newRedisMeta return a meta store using Redis.
// newRedisMeta("redis", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003/2")
func NewRedisMeta(driver, addr string, conf *Config) (MDS, error) {
	uri := driver + "://" + addr
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("url parse %s: %s", uri, err)
	}

	hosts := u.Host
	opt, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, fmt.Errorf("redis parse %s: %s", uri, err)
	}

	if opt.TLSConfig != nil {
		//TODO:TLS
		/*
			opt.TLSConfig.ServerName = tlsServerName // use the host of each connection as ServerName
			opt.TLSConfig.InsecureSkipVerify = skipVerify != ""
			if certFile != "" {
				cert, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					return nil, fmt.Errorf("get certificate error certFile:%s keyFile:%s error:%s", certFile, keyFile, err)
				}
				opt.TLSConfig.Certificates = []tls.Certificate{cert}
			}
			if caCertFile != "" {
				caCert, err := os.ReadFile(caCertFile)
				if err != nil {
					return nil, fmt.Errorf("read ca cert file error path:%s error:%s", caCertFile, err)
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				opt.TLSConfig.RootCAs = caCertPool
			}*/
	}
	if opt.Password == "" {
		opt.Password = os.Getenv("REDIS_PASSWORD")
	}
	if opt.Password == "" {
		opt.Password = os.Getenv("META_PASSWORD")
	}
	opt.MaxRetries = conf.Retries
	if opt.MaxRetries == 0 {
		opt.MaxRetries = -1 // Redis use -1 to disable retries
	}
	var rdb redis.UniversalClient
	/*
		//opt.MinRetryBackoff = minRetryBackoff
		//opt.MaxRetryBackoff = maxRetryBackoff
		//opt.ReadTimeout = readTimeout
		//opt.WriteTimeout = writeTimeout
		var rdb redis.UniversalClient
		var prefix string
		if strings.Contains(hosts, ",") && strings.Index(hosts, ",") < strings.Index(hosts, ":") {
			var fopt redis.FailoverOptions
			ps := strings.Split(hosts, ",")
			fopt.MasterName = ps[0]
			fopt.SentinelAddrs = ps[1:]
			_, port, _ := net.SplitHostPort(fopt.SentinelAddrs[len(fopt.SentinelAddrs)-1])
			if port == "" {
				port = "26379"
			}
			for i, addr := range fopt.SentinelAddrs {
				h, p, e := net.SplitHostPort(addr)
				if e != nil {
					fopt.SentinelAddrs[i] = net.JoinHostPort(addr, port)
				} else if p == "" {
					fopt.SentinelAddrs[i] = net.JoinHostPort(h, port)
				}
			}
			fopt.SentinelPassword = os.Getenv("SENTINEL_PASSWORD")
			fopt.DB = opt.DB
			fopt.Username = opt.Username
			fopt.Password = opt.Password
			fopt.TLSConfig = opt.TLSConfig
			fopt.MaxRetries = opt.MaxRetries
			fopt.MinRetryBackoff = opt.MinRetryBackoff
			fopt.MaxRetryBackoff = opt.MaxRetryBackoff
			fopt.DialTimeout = opt.DialTimeout
			fopt.ReadTimeout = opt.ReadTimeout
			fopt.WriteTimeout = opt.WriteTimeout
			fopt.PoolFIFO = opt.PoolFIFO               // default: false
			fopt.PoolSize = opt.PoolSize               // default: GOMAXPROCS * 10
			fopt.PoolTimeout = opt.PoolTimeout         // default: ReadTimeout + 1 second.
			fopt.MinIdleConns = opt.MinIdleConns       // disable by default
			fopt.MaxIdleConns = opt.MaxIdleConns       // disable by default
			fopt.MaxActiveConns = opt.MaxActiveConns   // default: 0, no limit
			fopt.ConnMaxIdleTime = opt.ConnMaxIdleTime // default: 30 minutes
			fopt.ConnMaxLifetime = opt.ConnMaxLifetime // disable by default
			if conf.ReadOnly {
				// NOTE: RouteByLatency and RouteRandomly are not supported since they require cluster client
				fopt.ReplicaOnly = routeRead == "replica"
			}
			rdb = redis.NewFailoverClient(&fopt)
		} else {
			if !strings.Contains(hosts, ",") {
				c := redis.NewClient(opt)
				info, err := c.ClusterInfo(Background()).Result()
				if err != nil && strings.Contains(err.Error(), "cluster mode") || err == nil && strings.Contains(info, "cluster_state:") {
					logger.Infof("redis %s is in cluster mode", hosts)
				} else {
					rdb = c
				}
			}
			if rdb == nil {
				var copt redis.ClusterOptions
				copt.Addrs = strings.Split(hosts, ",")
				copt.MaxRedirects = 1
				copt.Username = opt.Username
				copt.Password = opt.Password
				copt.TLSConfig = opt.TLSConfig
				copt.MaxRetries = opt.MaxRetries
				copt.MinRetryBackoff = opt.MinRetryBackoff
				copt.MaxRetryBackoff = opt.MaxRetryBackoff
				copt.DialTimeout = opt.DialTimeout
				copt.ReadTimeout = opt.ReadTimeout
				copt.WriteTimeout = opt.WriteTimeout
				copt.PoolFIFO = opt.PoolFIFO               // default: false
				copt.PoolSize = opt.PoolSize               // default: GOMAXPROCS * 10
				copt.PoolTimeout = opt.PoolTimeout         // default: ReadTimeout + 1 second.
				copt.MinIdleConns = opt.MinIdleConns       // disable by default
				copt.MaxIdleConns = opt.MaxIdleConns       // disable by default
				copt.MaxActiveConns = opt.MaxActiveConns   // default: 0, no limit
				copt.ConnMaxIdleTime = opt.ConnMaxIdleTime // default: 30 minutes
				copt.ConnMaxLifetime = opt.ConnMaxLifetime // disable by default
				if conf.ReadOnly {
					switch routeRead {
					case "random":
						copt.RouteRandomly = true
					case "latency":
						copt.RouteByLatency = true
					case "replica":
						copt.ReadOnly = true
					default:
						// route to primary
					}
				}
				rdb = redis.NewClusterClient(&copt)
				prefix = fmt.Sprintf("{%d}", opt.DB)
			}
		}*/
	if strings.Contains(hosts, ",") && strings.Index(hosts, ",") < strings.Index(hosts, ":") {
		logger.Infof("redis %s is in sentinel mode, it is not implemented, so will use the first host", hosts)
		hosts, err = extractBetweenCommas(hosts)
	}
	if !strings.Contains(hosts, ",") {
		logger.Infof("redis host[%s] is in single service mode", hosts)
		c := redis.NewClient(opt)
		info, err := c.ClusterInfo(context.Background()).Result()
		if err != nil && strings.Contains(err.Error(), "cluster mode") || err == nil && strings.Contains(info, "cluster_state:") {
			logger.Infof("redis %s is in cluster mode", hosts)
		} else {
			logger.Infof("redis %s is in single mode", hosts)
		}
		rdb = c
	} else {
		logger.Fatalf("failed to find any valid host in redis hosts")
		return nil, errors.New("failed to find any valid host in redis hosts")
	}
	prefix := fmt.Sprintf("DB%d", opt.DB)
	m := MDSRedis{
		rdb:          rdb,
		prefix:       prefix,
		bucketPrefix: prefix + "BUK",
		ObjectPrefic: prefix + "OBJ",
	}
	//m.en = m
	m.checkServerConfig()
	m.Init(&m.metesetting, true)
	return &m, nil
}
func (m *MDSRedis) checkServerConfig() {
	rawInfo, err := m.rdb.Info(context.Background()).Result()
	if err != nil {
		logger.Warnf("parse info: %s", err)
		return
	}
	rInfo, err := checkRedisInfo(rawInfo)
	if err != nil {
		logger.Warnf("parse info: %s", err)
	}
	if rInfo.storageProvider == "" && rInfo.maxMemoryPolicy != "" && rInfo.maxMemoryPolicy != "noeviction" {
		logger.Warnf("maxmemory_policy is %q,  we will try to reconfigure it to 'noeviction'.", rInfo.maxMemoryPolicy)
		if _, err := m.rdb.ConfigSet(context.Background(), "maxmemory-policy", "noeviction").Result(); err != nil {
			logger.Errorf("try to reconfigure maxmemory-policy to 'noeviction' failed: %s", err)
		} else if result, err := m.rdb.ConfigGet(context.Background(), "maxmemory-policy").Result(); err != nil {
			logger.Warnf("get config maxmemory-policy failed: %s", err)
		} else if len(result) == 1 && result["maxmemory-policy"] != "noeviction" {
			logger.Warnf("reconfigured maxmemory-policy to 'noeviction', but it's still %s", result["maxmemory-policy"])
		} else {
			logger.Infof("set maxmemory-policy to 'noeviction' successfully")
		}
	}
	start := time.Now()
	_, err = m.rdb.Ping(context.Background()).Result()
	if err != nil {
		logger.Errorf("Ping redis: %s", err.Error())
		return
	}
	logger.Infof("Ping redis latency: %s", time.Since(start))
}

func extractBetweenCommas(s string) (string, error) {
	first := strings.Index(s, ",")

	second := strings.Index(s[first+1:], ",")

	second += first + 1

	return s[first+1 : second], nil
}

func (m *MDSRedis) Shutdown() error {
	return m.rdb.Close()
}

func (m *MDSRedis) Name() string {
	return "redis"
}

func (m *MDSRedis) setting() string {
	return m.prefix + "setting"
}

func (m *MDSRedis) Init(format *Format, force bool) error {
	ctx := context.Background()
	body, err := m.rdb.Get(ctx, m.setting()).Bytes()
	if err != nil && err != redis.Nil {
		return err
	}
	if err == nil {
		err = json.Unmarshal(body, format)
		if err != nil {
			return errors.New("existing format is broken: " + err.Error())
		}

	} else if err == redis.Nil {
		// create new format
		format = &Format{
			Name:             "S3Store",
			UUID:             uuid.New().String(),
			Storage:          "minio",
			StorageClass:     "Single",
			BucketPrefix:     m.bucketPrefix,
			AccessKey:        "default",
			SecretKey:        "default",
			SessionToken:     "default",
			BlockSize:        4 * 1024 * 1024,
			Compression:      "none",
			Shards:           1,
			HashPrefix:       false,
			Capacity:         0,
			Inodes:           0,
			EncryptKey:       "default",
			EncryptAlgo:      "none",
			KeyEncrypted:     false,
			UploadLimit:      0,
			DownloadLimit:    0,
			TrashDays:        0,
			MetaVersion:      1,
			MinClientVersion: "1.0.0",
			MaxClientVersion: "1.0.0",
			EnableACL:        false,
			RangerRestUrl:    "http://localhost:9000",
			RangerService:    "default",
		}
		jsonDataIndent, err := json.MarshalIndent(format, "", "  ")
		if err != nil {
			return err
		}
		if err = m.rdb.Set(ctx, m.setting(), jsonDataIndent, 0).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (m *MDSRedis) MakeBucket(bucket string) error {
	ctx := context.Background()

	exists, err := m.rdb.HExists(ctx, BucketsKey, bucket).Result()
	if err != nil {
		logger.Errorf("MakeBucket:failed to check exist for bucket:%s, err:%s", bucket, err)
		return err
	}

	if exists {
		logger.Warnf("MakeBucket:bucket:%s already exists", bucket)
		return err
	}

	bucketinfo := newBucketInfo(bucket)
	jsonData, err := json.Marshal(bucketinfo)
	if err != nil {
		return err
	}
	logger.Infof("MDSRedis::MakeBucket[%s]: %s", bucket, jsonData)
	err = m.rdb.HSet(ctx, BucketsKey, bucket, jsonData).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HSet bucket[%s] to %s: %s", bucket, BucketsKey, err)
		return err
	}
	return nil
}

func (m *MDSRedis) DelBucket(bucket string) error {
	ctx := context.Background()
	objCount, err := m.rdb.HLen(ctx, bucket).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HLen bucket[%s]: %s", bucket, err)
		return err
	}
	if objCount > 0 {
		logger.Errorf("the bucket:%s is not empty(%d objects left)", bucket, objCount)
		return fmt.Errorf("the bucket:%s is not empty(%d objects left)", bucket, objCount)
	}
	err = m.rdb.Del(ctx, bucket).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to DelBucket[%s]: %s", bucket, err)
		return err
	}
	err = m.rdb.HDel(ctx, BucketsKey, bucket).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to DelBucket[%s] in %s: %s", bucket, BucketsKey, err)
		return err
	}
	logger.Tracef("MDSRedis::successfully DelBucket[%s]", bucket)
	return nil
}

func (m *MDSRedis) ListBuckets() ([]minio.BucketInfo, error) {
	ctx := context.Background()
	buckets, err := m.rdb.HGetAll(ctx, BucketsKey).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to ListBuckets: %s", err)
		return nil, err
	}
	var result []minio.BucketInfo

	for bucket, value := range buckets {
		var bucketinfo BucketInfo
		if err := json.Unmarshal([]byte(value), &bucketinfo); err != nil {
			logger.Errorf("MDSRedis::ListBuckets: failed to unmarshal bucket info(%s): %s", bucket, err)
			continue
		}
		result = append(result, minio.BucketInfo{Name: bucketinfo.Name,
			Created: bucketinfo.Created,
		})
		logger.Tracef("MDSRedis::ListBuckets: %s: %s", bucket, value)
	}

	return result, nil
}

func (m *MDSRedis) ListObjects(bucket string, prefix string) (result []minio.ObjectInfo, err error) {
	ctx := context.Background()
	objects, err := m.rdb.HGetAll(ctx, bucket).Result()
	if err != nil {
		logger.Errorf("failed to HGetAll bucket: %s, err: %s", bucket, err)
		return nil, err
	}

	for key, value := range objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		var objectInfo minio.ObjectInfo
		if err := json.Unmarshal([]byte(value), &objectInfo); err != nil {
			logger.Errorf("failed to unmarshal object info [key:%s], err: %s", key, err)
			continue
		}
		result = append(result, objectInfo)
	}

	return result, nil
}

func (m *MDSRedis) PutObjectMeta(object minio.ObjectInfo) error {
	ctx := context.Background()

	jsondata, err := json.Marshal(object)
	if err != nil {
		return err
	}
	err = m.rdb.HSet(ctx, object.Bucket, object.Name, jsondata).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HSet object[%s] into bucket[%s] : %s", object.Name, object.Bucket, err)
		return err
	}

	return nil
}

func (m *MDSRedis) GetObjectMeta(object *minio.ObjectInfo) error {
	ctx := context.Background()
	logger.Tracef("GetObjectMeta:objectKey=%s", object.Name)
	obj_info, err := m.rdb.HGet(ctx, object.Bucket, object.Name).Result()
	if err != nil {
		return err
	}
	if len(obj_info) == 0 {
		return minio.ObjectNotFound{Bucket: object.Bucket, Object: object.Name}
	}

	return json.Unmarshal([]byte(obj_info), &object)
}

func (m *MDSRedis) DelObjectMeta(bucket string, obj string) ([]int64, error) {
	ctx := context.Background()
	logger.Tracef("GetObjectMeta:bucket:%s,object:%s", bucket, obj)
	objInfo := minio.ObjectInfo{
		Bucket: bucket,
		Name:   obj,
	}
	objKey := objInfo.Name
	err := m.GetObjectMeta(&objInfo)
	if err != nil {
		logger.Errorf("failed to GetObjectMeta object:%s", objKey)
		return nil, err
	}
	//get manifest
	mfid := objInfo.UserTags
	chunks, err := m.GetManifest(mfid)
	if err != nil {
		logger.Errorf("failed to GetManifest[%s], err:%s", mfid, err)
		return nil, err
	}
	//TODO: get all DObj?
	set := internal.NewInt64Set()
	for _, chunk := range chunks {
		set.Add(int64(chunk.DOid))
	}
	dobjIDs := set.Elements()
	//delete minifest
	err = m.rdb.Del(ctx, mfid).Err()
	if err != nil {
		logger.Errorf("failed to delete manifest:%s, err:%s", mfid, err)
		return dobjIDs, err
	}
	//delete objKey
	err = m.rdb.HDel(ctx, bucket, objKey).Err()
	if err != nil {
		logger.Errorf("failed to delete object[%s] meta, err:%s", objKey, err)
		return dobjIDs, err
	}
	return dobjIDs, nil
}

/*
	func (m *MDSRedis) extractObjectName(objInMDS string) (string, error) {
		//fin the second /
		first := strings.Index(objInMDS, "/")
		if first == -1 {
			return "", fmt.Errorf("Cannot find the 1st '/' in %s", objInMDS)
		}
		second := strings.Index(objInMDS[first+1:], "/")
		if second == -1 {
			return "", fmt.Errorf("Cannot find the 2nd '/' in %s", objInMDS)
		}
		secondPos := first + 1 + second
		logger.Tracef("extractObjectName:objInMDS:%s, result:%s", objInMDS, objInMDS[secondPos+1:])
		return objInMDS[secondPos+1:], nil
	}

	func (m *MDSRedis) objectKeyPattern(bucket string) string {
		return fmt.Sprintf("%s/%s/*", m.ObjectPrefic, bucket)
	}
*/
func (m *MDSRedis) BucketExist(bucket string) (bool, error) {
	ctx := context.Background()

	exists, err := m.rdb.HExists(ctx, BucketsKey, bucket).Result()
	if err != nil {
		logger.Errorf("MakeBucket:failed to check exist for bucket:%s, err:%s", bucket, err)
		return false, err
	}

	return exists, nil
}

func (m *MDSRedis) GetIncreasedDOID() (int64, error) {
	ctx := context.Background()

	id, err := m.rdb.Incr(ctx, doidkey).Result()
	if err != nil {
		return 0, err
	}
	return id, nil
}
func (m *MDSRedis) GetDObjNameInMDS(id int64) string {

	return fmt.Sprintf("%s%d", DOKeyWord, id)
}

func (m *MDSRedis) GetDOIDFromDObjName(objName string) (num int64, err error) {

	if len(objName) <= len(DOKeyWord) || objName[:len(DOKeyWord)] != DOKeyWord {
		return 0, fmt.Errorf("ObjName[%s] is not starting with %s", objName, DOKeyWord)
	}

	numStr := objName[len(DOKeyWord):]
	if numStr == "" {
		return 0, fmt.Errorf("there is no num")
	}

	num, err = strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the num[%s] is not valid: %w", numStr, err)
	}
	return
}

func (m *MDSRedis) GetIncreasedManifestID() (string, error) {
	ctx := context.Background()

	id, err := m.rdb.Incr(ctx, manifestKey).Result()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Manifest%d", id), nil
}

func (m *MDSRedis) WriteManifest(manifestid string, chunks []Chunk) error {
	ctx := context.Background()
	for _, chunk := range chunks {
		str, err := internal.SerializeToString(ChunkInManifest{FP: chunk.FP, Len: chunk.Len, DOid: chunk.DOid,
			OffInDOid: chunk.OffInDOid, LenInDOid: chunk.LenInDOid})
		_, err = m.rdb.RPush(ctx, manifestid, str).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MDSRedis) GetManifest(manifestid string) (chunks []ChunkInManifest, err error) {
	ctx := context.Background()
	// Get the list of chunk IDs from the manifest
	fps, err := m.rdb.LRange(ctx, manifestid, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// Retrieve each chunk's metadata
	for _, fp := range fps {

		var chunk ChunkInManifest
		if err := internal.DeserializeFromString(fp, &chunk); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func (m *MDSRedis) DedupFPs(chunks []Chunk) error {
	//pipe := m.rdb.Pipeline()

	for i, chunk := range chunks {

		fps := m.getFingerprint(chunk.FP)
		if fps != nil {
			chunks[i].DOid = fps.DOid
			chunks[i].LenInDOid = fps.LenInDOid
			chunks[i].OffInDOid = fps.OffInDOid
			logger.Tracef("DedupFPs:found existed fp:%s, LenInDOid:%d, OffInDOid:%d", internal.StringToHex(chunk.FP), chunks[i].LenInDOid, chunks[i].OffInDOid)
		}
	}
	return nil
}

func (m *MDSRedis) InsertFPs(chunks []ChunkInManifest) error {
	//pipe := m.rdb.Pipeline()

	for _, chunk := range chunks {
		//pipe.Set(ctx, getFPNameInMDS(chunk.FP), str, 0)
		err := m.setFingerprint(chunk.FP, FPValInMDS{DOid: chunk.DOid, OffInDOid: chunk.OffInDOid, LenInDOid: chunk.LenInDOid})
		if err != nil {
			return err
		}
		logger.Tracef("InsertFPs: fp[%s], DOid:%d", internal.StringToHex(chunk.FP), chunk.DOid)
	}
	//_, err := pipe.Exec(ctx)
	return nil
}

// 1: found, 0 is not
func (m *MDSRedis) getFingerprint(fp string) *FPValInMDS {
	ctx := context.Background()
	fp_val, err := m.rdb.HGet(ctx, FPCacheKey, fp).Result()
	if err != nil {
		if err == redis.Nil {
			//not existed
			return nil
		}
		logger.Errorf("setFingerprint: failed to HGet(%s). err:%s", internal.StringToHex(fp), err)
		return nil
	}
	fps := &FPValInMDS{}
	err = internal.DeserializeFromString(fp_val, fps)
	if err != nil {
		logger.Errorf("setFingerprint: failed to DeserializeFromString. err:%s", err)
		return nil
	}
	logger.Tracef("found fp:%s", internal.StringToHex(fp))
	return fps
}

func (m *MDSRedis) setFingerprint(fp string, dpval FPValInMDS) error {
	ctx := context.Background()
	str_val, err := internal.SerializeToString(dpval)
	if err != nil {
		logger.Errorf("setFingerprint: failed to SerializeToString. err:%s", err)
		return err
	}
	return m.rdb.HSet(ctx, FPCacheKey, fp, str_val).Err()
}
