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
	"github.com/zhengshuai-xiao/XlatorS/internal"
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
	KeyExists      = 1
	DOKeyWord      = "DataObj"
	FPCacheKey     = "FPCache"
	BucketsKey     = "Buckets"
	RefKeySuffix   = "Ref"
	DeletedDOIDKey = "deletedDOID"
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
			Name:             "XlatorS",
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

// MakeBucket creates a new bucket entry in the metadata store.
// It takes the actual bucket name and its associated namespace.
// It ensures that the bucket name is unique across all namespaces before creation.
func (m *MDSRedis) MakeBucket(bucket string) error {
	ctx := context.Background()

	// Check if the bucket already exists globally (in any namespace)
	// This prevents creating a bucket with the same name in different namespaces,
	// which might lead to confusion or conflicts in a global context.
	exists, err := m.rdb.HExists(ctx, BucketsKey, bucket).Result()
	if err != nil {
		logger.Errorf("MakeBucket: failed to check global existence for bucket[%s], err:%s", bucket, err)
		return err
	}
	if exists {
		logger.Warnf("MakeBucket: bucket[%s] already exists globally.", bucket)
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	}

	// Create new bucket info using the helper from types.go
	bucketinfo := newBucketInfo(bucket) // newBucketInfo is now in types.go
	//bucketinfo.Location = namespace     // Store the namespace as the bucket's location
	jsonData, err := json.Marshal(bucketinfo)
	if err != nil {
		return err
	}

	logger.Infof("MDSRedis::MakeBucket[%s] %s", bucket, jsonData)

	// Store the bucket metadata, associating it with the actual bucket name (not the full name with namespace)
	err = m.rdb.HSet(ctx, BucketsKey, bucket, jsonData).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HSet bucket[%s]: %s", bucket, err)
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
		logger.Errorf("MDSRedis::failed to DelBucket[%s] %s", bucket, err)
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

func (m *MDSRedis) PutObjectMeta(object minio.ObjectInfo, manifestList []ChunkInManifest) error {
	ctx := context.Background()
	ns, _, err := ParseNamespaceAndBucket(object.Bucket)
	if err != nil {
		logger.Errorf("failed to parse namespace and bucket: %s", err)
		return err
	}
	//write manifest
	logger.Tracef("MDSRedis::PutObjectMeta: writing manifest for object[%s], manifestname:%s, len:%d", object.Name, object.UserTags, len(manifestList))
	doidSet, err := m.writeManifestReturnDOidList(object.UserTags, manifestList)
	if err != nil {
		//cleanup the manifest, ignore if it is failed
		m.delManifest(ctx, object.UserTags)
		return err
	}
	//write reference
	err = m.AddReference(ns, doidSet.Elements(), object.Name)
	if err != nil {
		//cleanup the manifest, ignore if it is failed
		m.delManifest(ctx, object.UserTags)
		return err
	}
	//write object
	jsondata, err := json.Marshal(object)
	if err != nil {
		//cleanup the manifest, ignore if it is failed
		m.delManifest(ctx, object.UserTags)
		//cleanup the reference
		m.RemoveReference(ns, doidSet.Elements(), object.Name)
		return err
	}
	err = m.rdb.HSet(ctx, object.Bucket, object.Name, jsondata).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HSet object[%s] into bucket[%s] : %s", object.Name, object.Bucket, err)
		return err
	}

	return nil
}

func (m *MDSRedis) GetObjectInfo(bucket string, obj string) (minio.ObjectInfo, error) {
	var objInfo minio.ObjectInfo
	objInfo.Bucket = bucket
	objInfo.Name = obj

	err := m.GetObjectMeta(&objInfo)
	if err != nil {
		logger.Errorf("GetObjectInfo:failed to GetObjectInfo[%s] err: %s", objInfo.Name, err)
		return objInfo, err
	}
	return objInfo, nil
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

// DelObjectMeta deletes an object's metadata and all associated references from the metadata store.
// This is a multi-step process:
// 1. Fetches the object's metadata to retrieve its manifest ID (stored in UserTags).
// 2. Reads the manifest to get the list of all Data Object IDs (DOIDs) that make up this object.
// 3. Deletes the manifest itself.
// 4. Removes the object's reference from each of the associated DOIDs.
// 5. If a DOID's reference count drops to zero after this removal, its ID is collected.
// 6. Deletes the primary object metadata entry from the bucket's hash.
// It returns a slice of DOIDs that have become dereferenced (ref count is zero) and are now eligible for garbage collection.
func (m *MDSRedis) DelObjectMeta(bucket string, obj string) (dereferencedDObjIDs []uint64, err error) {
	ctx := context.Background()
	logger.Tracef("GetObjectMeta:bucket:%s,object:%s", bucket, obj)
	objInfo := minio.ObjectInfo{
		Bucket: bucket,
		Name:   obj,
	}

	objKey := objInfo.Name
	// Step 1: Retrieve the object's metadata to find its manifest ID.
	err = m.GetObjectMeta(&objInfo)
	if err != nil {
		logger.Errorf("failed to GetObjectMeta object:%s", objKey)
		return nil, err
	}
	// The manifest ID is stored in the UserTags field.
	mfid := objInfo.UserTags
	// Step 2: Get the list of DOIDs from the manifest.
	_, doidSet, err := m.getManifestAndDOIDSet(mfid)
	if err != nil {
		logger.Errorf("failed to GetManifest[%s], err:%s", mfid, err)
		return nil, err
	}

	dobjIDs := doidSet.Elements()
	// Step 3: Delete the manifest list from Redis.
	err = m.delManifest(ctx, mfid)
	if err != nil {
		logger.Errorf("failed to delete manifest:%s, err:%s", mfid, err)
		// The state is now inconsistent. Return the found DOIDs with the error.
		return dobjIDs, err
	}
	// Step 4 & 5: Remove the object's reference from all associated DOIDs and get the list of dereferenced ones.
	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("failed to parse namespace and bucket: %s for object:%s", err, objInfo.Name)
		return nil, err
	}
	//dereferencedDObjIDs means the DOID in this list is no one to referenced, can be deleted from backend storage
	dereferencedDObjIDs, err = m.RemoveReference(ns, dobjIDs, objKey)
	if err != nil {
		logger.Errorf("failed to RemoveReference for object:%s, err:%s", objKey, err)
		return nil, err
	}
	//delete relevent fp in FPCache

	// Step 6: Delete the object's metadata key from the bucket's hash.
	err = m.rdb.HDel(ctx, bucket, objKey).Err()
	if err != nil {
		logger.Errorf("failed to delete object[%s] meta, err:%s", objKey, err)
		return dereferencedDObjIDs, err
	}
	return dereferencedDObjIDs, nil
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
func (m *MDSRedis) GetDObjNameInMDS(id uint64) string {

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

func (m *MDSRedis) WriteManifest(manifestid string, manifestList []ChunkInManifest) error {
	ctx := context.Background()
	for _, chunk := range manifestList {
		str, err := internal.SerializeToString(chunk)
		if err != nil {
			logger.Errorf("MDSRedis::failed to SerializeToString chunk[%v] : %s", chunk, err)
			return err
		}
		_, err = m.rdb.RPush(ctx, manifestid, str).Result()
		if err != nil {
			logger.Errorf("MDSRedis::failed to RPush chunk[%v] into manifest[%s] : %s", chunk, manifestid, err)
			return err
		}
	}
	return nil
}

func (m *MDSRedis) writeManifestReturnDOidList(manifestid string, manifestList []ChunkInManifest) (doidSet *internal.UInt64Set, err error) {
	ctx := context.Background()
	doidSet = internal.NewUInt64Set()
	for _, chunk := range manifestList {

		doidSet.Add(chunk.DOid)

		str, err := internal.SerializeToString(chunk)
		if err != nil {
			logger.Errorf("MDSRedis::failed to SerializeToString chunk[%v] : %s", chunk, err)
			return nil, err
		}
		_, err = m.rdb.RPush(ctx, manifestid, str).Result()
		if err != nil {
			logger.Errorf("MDSRedis::failed to RPush chunk[%v] into manifest[%s] : %s", chunk, manifestid, err)
			return nil, err
		}
	}
	return doidSet, nil
}

func (m *MDSRedis) delManifest(ctx context.Context, mfid string) (err error) {
	//delete minifest
	err = m.rdb.Del(ctx, mfid).Err()
	if err != nil {
		return fmt.Errorf("failed to delete manifest:%s, err:%s", mfid, err)
	}
	return nil
}

func (m *MDSRedis) GetObjectManifest(bucket, object string) (chunks []ChunkInManifest, err error) {
	//ctx := context.Background()
	// Get the manifest ID for the object
	objInfo := minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}
	objKey := objInfo.Name
	err = m.GetObjectMeta(&objInfo)
	if err != nil {
		logger.Errorf("failed to GetObjectMeta object:%s", objKey)
		return nil, err
	}
	//get manifest
	manifestid := objInfo.UserTags

	// Get the manifest chunks
	logger.Tracef("manifestid:%s", manifestid)
	chunks, err = m.GetManifest(manifestid)
	if err != nil {
		logger.Errorf("GetObjectManifest: failed to get manifest[%s] err: %s", manifestid, err)
		return nil, err
	}
	return chunks, nil
}

func (m *MDSRedis) GetManifest(manifestid string) (chunks []ChunkInManifest, err error) {
	ctx := context.Background()
	// Get the list of chunk IDs from the manifest
	fps, err := m.rdb.LRange(ctx, manifestid, 0, -1).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to LRange manifest[%s] : %s", manifestid, err)
		return nil, err
	}

	// Retrieve each chunk's metadata
	for _, fp := range fps {

		var chunk ChunkInManifest
		if err := internal.DeserializeFromString(fp, &chunk); err != nil {
			logger.Errorf("MDSRedis::failed to DeserializeFromString chunk[%v] : %s", chunk, err)
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func (m *MDSRedis) getManifestAndDOIDSet(manifestid string) (chunks []ChunkInManifest, doidSet *internal.UInt64Set, err error) {
	ctx := context.Background()
	doidSet = internal.NewUInt64Set()
	// Get the list of chunk IDs from the manifest
	fps, err := m.rdb.LRange(ctx, manifestid, 0, -1).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to LRange manifest[%s] : %s", manifestid, err)
		return nil, nil, err
	}

	// Retrieve each chunk's metadata
	for _, fp := range fps {

		var chunk ChunkInManifest
		if err := internal.DeserializeFromString(fp, &chunk); err != nil {
			logger.Errorf("MDSRedis::failed to DeserializeFromString chunk[%v] : %s", chunk, err)
			return nil, nil, err
		}
		chunks = append(chunks, chunk)
		doidSet.Add(chunk.DOid)
	}
	return chunks, doidSet, nil
}

func (m *MDSRedis) DedupFPs(namespace string, chunks []Chunk) error {
	//pipe := m.rdb.Pipeline()

	for i, chunk := range chunks {

		fps := m.getFingerprint(namespace, chunk.FP)
		if fps != nil {
			chunks[i].DOid = fps.DOid
			chunks[i].Deduped = true
			//chunks[i].LenInDOid = fps.LenInDOid
			//chunks[i].OffInDOid = fps.OffInDOid
			logger.Tracef("DedupFPs:found existed fp:%s", internal.StringToHex(chunk.FP))
		}
	}
	return nil
}

func (m *MDSRedis) InsertFPs(namespace string, chunks []ChunkInManifest) error {
	//pipe := m.rdb.Pipeline()

	for _, chunk := range chunks {
		//pipe.Set(ctx, getFPNameInMDS(chunk.FP), str, 0)
		err := m.setFingerprint(namespace, chunk.FP, FPValInMDS{DOid: chunk.DOid})
		if err != nil {
			return err
		}
		logger.Tracef("InsertFPs: fp[%s], DOid:%d", internal.StringToHex(chunk.FP), chunk.DOid)
	}
	//_, err := pipe.Exec(ctx)
	return nil
}

func (m *MDSRedis) DedupFPsBatch(namespace string, chunks []Chunk) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	deleteDOIDKey := GetDeletedDOIDKey(namespace)
	// Use a WATCH transaction to ensure we get a consistent view of the fingerprints.
	// If the fingerprint cache is modified concurrently (e.g., by RemoveFPs),
	// the transaction will fail and retry, preventing decisions based on stale data.
	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(chunks) == 0 {
			return nil
		}

		fps := make([]string, len(chunks))
		for i := range chunks {
			fps[i] = chunks[i].FP
		}

		// HMGet is more efficient for batch lookups.
		vals, err := tx.HMGet(ctx, fpCache, fps...).Result()
		if err != nil {
			// redis.Nil is not returned by HMGet. Instead, the slice contains nil for non-existent keys.
			return err
		}

		for i := range chunks {
			if vals[i] == nil {
				// Fingerprint not found in cache.
				chunks[i].Deduped = false
				continue
			}

			doidStr, ok := vals[i].(string)
			if !ok || doidStr == "" {
				// This case should ideally not happen if data is consistent.
				logger.Warnf("DedupFPsBatch: unexpected or empty value for fp %s", internal.StringToHex(chunks[i].FP))
				chunks[i].Deduped = false
				continue
			}

			DOid, err := strconv.ParseUint(doidStr, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse DOID '%s' for fp %s: %w", doidStr, internal.StringToHex(chunks[i].FP), err)
			}
			exist, err := m.IsDOIDDeleted(namespace, DOid)
			if err != nil {
				return err
			}
			if exist {
				logger.Tracef("the fp[%s]'s Data object[id:%d] is in deleted list, so skip it", internal.StringToHex(chunks[i].FP), DOid)
				chunks[i].Deduped = false
				continue
			}
			chunks[i].DOid = DOid
			chunks[i].Deduped = true
			logger.Tracef("DedupFPsBatch: found existing fp:%s in %s, DOid: %d", internal.StringToHex(chunks[i].FP), fpCache, DOid)
		}
		return nil
	}, fpCache, deleteDOIDKey)

	if err != nil {
		logger.Errorf("DedupFPsBatch: transaction failed for namespace %s: %v", namespace, err)
	}
	return err
}

func (m *MDSRedis) InsertFPsBatch(namespace string, chunks []ChunkInManifest) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)

	// Use a WATCH transaction to prevent race conditions with concurrent deletes or inserts.
	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(chunks) == 0 {
			return nil
		}
		pipe := tx.TxPipeline()
		for _, chunk := range chunks {
			// HSetNX is used to avoid overwriting a fingerprint that might have been
			// inserted by a concurrent process.
			// Using HSet will overwrite any existing value,
			// which can lead to race conditions and data inconsistency.
			//But here we should use HSet
			pipe.HSet(ctx, fpCache, chunk.FP, strconv.FormatUint(chunk.DOid, 10))
			logger.Tracef("InsertFPsBatch: fp[%s], DOid:%d", internal.StringToHex(chunk.FP), chunk.DOid)
		}
		_, err := pipe.Exec(ctx)
		return err
	}, fpCache)

	if err != nil {
		logger.Errorf("InsertFPsBatch: transaction failed for namespace %s: %v", namespace, err)
	}
	return err
}

// RemoveFPs removes a batch of fingerprints from the cache for a specific namespace.
// It only removes a fingerprint if its associated Data Object ID matches the provided DOid.
// This is crucial to prevent deleting a fingerprint that has been reused by a newer Data Object.
// This operation is atomic, using a Redis WATCH transaction to effectively lock the fingerprint
// cache for the given namespace during the operation.
func (m *MDSRedis) RemoveFPs(namespace string, FPs []string, DOid uint64) error {
	ctx := context.Background()
	fpCacheKey := GetFingerprintCache(namespace)
	doidStr := strconv.FormatUint(DOid, 10)

	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		if len(FPs) == 0 {
			return nil
		}

		// Get all current DOIDs for the given FPs in one command to be efficient.
		storedDoidVals, err := tx.HMGet(ctx, fpCacheKey, FPs...).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		var fpsToDelete []string
		for i, fp := range FPs {
			// storedDoidVals[i] will be nil if the key does not exist in the hash.
			if storedDoidVals[i] == nil {
				logger.Warnf("RemoveFPs: fingerprint %s not found in cache %s, skipping.", internal.StringToHex(fp), fpCacheKey)
				continue
			}

			storedDoidStr, ok := storedDoidVals[i].(string)
			if !ok {
				// This should not happen with Redis string values.
				logger.Errorf("RemoveFPs: unexpected type for fingerprint %s value in cache %s", internal.StringToHex(fp), fpCacheKey)
				continue
			}

			// Only add the FP to the deletion list if the stored DOid matches the expected one.
			if storedDoidStr == doidStr {
				fpsToDelete = append(fpsToDelete, fp)
			} else {
				// This is a valid scenario where the fingerprint has been overwritten by a new
				// data object. We must not delete it.
				logger.Warnf("RemoveFPs: fingerprint %s in cache %s has a different DOID (%s) than expected (%s). Not deleting.", internal.StringToHex(fp), fpCacheKey, storedDoidStr, doidStr)
			}
		}

		if len(fpsToDelete) > 0 {
			// Use a transaction pipeline to delete all matching FPs at once.
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HDel(ctx, fpCacheKey, fpsToDelete...)
				return nil
			})
			return err
		}

		return nil
	}, fpCacheKey)

	if err != nil {
		logger.Errorf("RemoveFPs: transaction failed for namespace %s: %v", namespace, err)
	}

	return err
}

func (m *MDSRedis) getFingerprint(namespace string, fp string) *FPValInMDS {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	doidStr, err := m.rdb.HGet(ctx, fpCache, fp).Result()
	if err != nil {
		if err == redis.Nil {
			//not existed
			return nil
		}
		logger.Errorf("getFingerprint: failed to HGet(%s). err:%s", internal.StringToHex(fp), err)
		return nil
	}
	fps := &FPValInMDS{}
	fps.DOid, err = strconv.ParseUint(doidStr, 10, 64)
	if err != nil {
		logger.Errorf("getFingerprint: failed to ParseUint. err:%s", err)
		return nil
	}
	logger.Tracef("found fp:%s", internal.StringToHex(fp))
	return fps
}

func (m *MDSRedis) setFingerprint(namespace string, fp string, dpval FPValInMDS) error {
	ctx := context.Background()
	fpCache := GetFingerprintCache(namespace)
	str_val, err := internal.SerializeToString(dpval)
	if err != nil {
		logger.Errorf("setFingerprint: failed to SerializeToString. err:%s", err)
		return err
	}
	return m.rdb.HSet(ctx, fpCache, fp, str_val).Err()
}

// AddReference adds an object reference to multiple Data Objects in a batch.
// It uses a Redis WATCH transaction to ensure atomicity.
// For each dataObjectID, it retrieves the current reference list (a comma-separated string),
// and appends the objectName if it does not already exist.
// Finally, it uses a Pipeline to update all changes in a batch.
func (m *MDSRedis) AddReference(namespace string, dataObjectIDs []uint64, objectName string) error {
	ctx := context.Background()
	refKey := GetRefKey(namespace)

	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		newValues := make(map[string]string)
		for _, dobjID := range dataObjectIDs {
			dobjIDStr := strconv.FormatUint(dobjID, 10)
			val, err := tx.HGet(ctx, refKey, dobjIDStr).Result()
			if err != nil && err != redis.Nil {
				return err // Real error, fail transaction
			}

			var newVal string
			if err == redis.Nil {
				// First reference
				newVal = objectName
			} else {
				objNames := strings.Split(val, ",")
				found := false
				for _, name := range objNames {
					if name == objectName {
						found = true
						break
					}
				}
				if found {
					continue // Already exists, skip this one
				}
				newVal = val + "," + objectName
			}
			newValues[dobjIDStr] = newVal
		}

		if len(newValues) > 0 {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				for id, val := range newValues {
					pipe.HSet(ctx, refKey, id, val)
				}
				return nil
			})
			return err
		}
		return nil
	}, refKey)

	if err != nil {
		logger.Errorf("AddReference: failed batch transaction for objName %s: %v", objectName, err)
	}
	return err
}

// RemoveReference removes an object reference from multiple Data Objects in a batch.
// It uses a Redis WATCH transaction to ensure atomicity.
// The function iterates through all dataObjectIDs, removing the specified objectName from each.
// If a Data Object's reference list becomes empty after removal, its field is deleted from the hash,
// and its ID is added to the returned list.
//
// The return value, dereferencedDObjIDs, is a []uint64 slice containing the IDs of all Data Objects
// whose reference count dropped to zero during this operation. This list can be used for subsequent
// garbage collection (GC).
func (m *MDSRedis) RemoveReference(namespace string, dataObjectIDs []uint64, objectName string) (dereferencedDObjIDs []uint64, err error) {
	ctx := context.Background()
	refKey := GetRefKey(namespace)

	err = m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		updates := make(map[string]interface{})
		deletes := make([]string, 0)
		dereferencedDObjIDs = nil // Reset for each transaction attempt

		for _, dobjID := range dataObjectIDs {
			dobjIDStr := strconv.FormatUint(dobjID, 10)
			val, err := tx.HGet(ctx, refKey, dobjIDStr).Result()
			if err == redis.Nil {
				logger.Warnf("RemoveReference: data object %d not found in ref map for namespace %s", dobjID, namespace)
				continue // Not an error, just nothing to do for this ID
			}
			if err != nil {
				return err // Real error, fail transaction
			}

			objNames := strings.Split(val, ",")

			found := false
			var newObjNames []string
			for _, name := range objNames {
				if name != objectName {
					newObjNames = append(newObjNames, name)
				} else {
					found = true
				}
			}

			if !found {
				logger.Warnf("RemoveReference: object name %s not found for data object %d", objectName, dobjID)
				continue // Not an error, reference was not present for this ID
			}

			if len(newObjNames) == 0 {
				deletes = append(deletes, dobjIDStr)
				dereferencedDObjIDs = append(dereferencedDObjIDs, dobjID)
			} else {
				updates[dobjIDStr] = strings.Join(newObjNames, ",")
			}
		}

		if len(updates) > 0 || len(deletes) > 0 {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if len(updates) > 0 {
					pipe.HSet(ctx, refKey, updates)
				}
				if len(deletes) > 0 {
					pipe.HDel(ctx, refKey, deletes...)
				}
				return nil
			})
			return err
		}
		return nil
	}, refKey)

	if err != nil {
		logger.Errorf("RemoveReference: failed batch transaction for objName %s: %v", objectName, err)
		return nil, err
	}
	return dereferencedDObjIDs, nil
}

// AddDeletedDOIDs adds a list of Data Object IDs to the set of deleted DOIDs for a given namespace.
// This set is used by a background garbage collection process.
// The operation is performed within a transaction to ensure atomicity against concurrent operations
// that might be reading this set (e.g., DedupFPsBatch).
func (m *MDSRedis) AddDeletedDOIDs(namespace string, doids []uint64) error {
	if len(doids) == 0 {
		return nil
	}

	ctx := context.Background()
	key := GetDeletedDOIDKey(namespace)

	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		// Convert []uint64 to []interface{} for SAdd
		members := make([]interface{}, len(doids))
		for i, doid := range doids {
			members[i] = doid
		}

		pipe := tx.TxPipeline()
		pipe.SAdd(ctx, key, members...)
		_, err := pipe.Exec(ctx)
		return err
	}, key)

	if err != nil {
		logger.Errorf("AddDeletedDOIDs: transaction failed to add DOIDs to set %s: %v", key, err)
		return err
	}
	logger.Tracef("AddDeletedDOIDs: added %d DOIDs to set %s", len(doids), key)

	return nil
}

// GetAllNamespaces retrieves a list of all unique namespaces by inspecting all bucket names.
func (m *MDSRedis) GetAllNamespaces() ([]string, error) {
	ctx := context.Background()
	buckets, err := m.rdb.HKeys(ctx, BucketsKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No buckets yet
		}
		logger.Errorf("GetAllNamespaces: failed to get bucket keys: %v", err)
		return nil, err
	}

	nsSet := make(map[string]struct{})
	for _, bucketName := range buckets {
		ns, _, err := ParseNamespaceAndBucket(bucketName)
		if err != nil {
			logger.Warnf("GetAllNamespaces: could not parse namespace from bucket '%s', skipping: %v", bucketName, err)
			continue
		}
		nsSet[ns] = struct{}{}
	}

	namespaces := make([]string, 0, len(nsSet))
	for ns := range nsSet {
		namespaces = append(namespaces, ns)
	}

	return namespaces, nil
}

// IsDOIDDeleted checks if a specific DOID is present in the deleted DOID set for a given namespace.
// It returns true if the DOID is marked for deletion, false otherwise.
func (m *MDSRedis) IsDOIDDeleted(namespace string, doid uint64) (bool, error) {
	ctx := context.Background()
	key := GetDeletedDOIDKey(namespace)
	doidStr := strconv.FormatUint(doid, 10)

	isMember, err := m.rdb.SIsMember(ctx, key, doidStr).Result()
	if err != nil {
		logger.Errorf("IsDOIDDeleted: failed to check SISMEMBER for DOID %d in set %s: %v", doid, key, err)
		return false, err
	}

	return isMember, nil
}

// GetRandomDeletedDOIDs retrieves a specified number of random DOIDs from the deleted set without removing them.
func (m *MDSRedis) GetRandomDeletedDOIDs(namespace string, count int64) ([]uint64, error) {
	if count <= 0 {
		return nil, nil
	}
	ctx := context.Background()
	key := GetDeletedDOIDKey(namespace)

	doidStrs, err := m.rdb.SRandMemberN(ctx, key, count).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Set is empty
		}
		logger.Errorf("GetRandomDeletedDOIDs: failed to SRANDMEMBER from %s: %v", key, err)
		return nil, err
	}

	if len(doidStrs) == 0 {
		return nil, nil
	}

	doids := make([]uint64, 0, len(doidStrs))
	for _, s := range doidStrs {
		doid, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			logger.Warnf("GetRandomDeletedDOIDs: failed to parse DOID string '%s', skipping: %v", s, err)
			continue
		}
		doids = append(doids, doid)
	}

	return doids, nil
}

// RemoveSpecificDeletedDOIDs removes a list of specific DOIDs from the deleted set.
func (m *MDSRedis) RemoveSpecificDeletedDOIDs(namespace string, doids []uint64) error {
	if len(doids) == 0 {
		return nil
	}

	ctx := context.Background()
	key := GetDeletedDOIDKey(namespace)

	// Convert []uint64 to []interface{} for SRem
	members := make([]interface{}, len(doids))
	for i, doid := range doids {
		members[i] = doid
	}

	err := m.rdb.SRem(ctx, key, members...).Err()
	if err != nil {
		logger.Errorf("RemoveSpecificDeletedDOIDs: failed to remove DOIDs from set %s: %v", key, err)
		return err
	}
	logger.Tracef("RemoveSpecificDeletedDOIDs: removed %d DOIDs from set %s", len(doids), key)

	return nil
}
