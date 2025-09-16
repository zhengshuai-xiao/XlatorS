// Copyright 2025 zhengshuai.xiao@outlook.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
package dedup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	FPCacheKey     = "FPCache"
	BucketsKey     = "Buckets"
	RefKeySuffix   = "Ref"
	DeletedDCIDKey = "deletedDCID"
)

const addRefScript = `
-- KEYS[1]: the hash key (e.g., "namespace.Ref")
-- ARGV[1]: the object name to add
-- ARGV[2...]: the list of dobjID strings
local ref_key = KEYS[1]
local obj_name_to_add = ARGV[1]
for i = 2, #ARGV do
    local dobj_id = ARGV[i]
    local current_refs_str = redis.call('HGET', ref_key, dobj_id)
    if not current_refs_str or current_refs_str == '' then
        redis.call('HSET', ref_key, dobj_id, obj_name_to_add)
    else
        local found = false
        for ref in string.gmatch(current_refs_str, "([^,]+)") do
            if ref == obj_name_to_add then
                found = true
                break
            end
        end
        if not found then
            local new_refs_str = current_refs_str .. ',' .. obj_name_to_add
            redis.call('HSET', ref_key, dobj_id, new_refs_str)
        end
    end
end
return 1`

const removeRefScript = `
-- KEYS[1]: the hash key (e.g., "namespace.Ref")
-- ARGV[1]: the object name to remove
-- ARGV[2...]: the list of dobjID strings
local ref_key = KEYS[1]
local obj_name_to_remove = ARGV[1]
local dereferenced_dobj_ids = {}
for i = 2, #ARGV do
    local dobj_id = ARGV[i]
    local current_refs_str = redis.call('HGET', ref_key, dobj_id)
    if current_refs_str then
        local new_refs = {}
        local found = false
        for ref in string.gmatch(current_refs_str, "([^,]+)") do
            if ref == obj_name_to_remove then
                found = true
            else
                table.insert(new_refs, ref)
            end
        end
        if found then
            if #new_refs == 0 then
                redis.call('HDEL', ref_key, dobj_id)
                table.insert(dereferenced_dobj_ids, dobj_id)
            else
                redis.call('HSET', ref_key, dobj_id, table.concat(new_refs, ','))
            end
        end
    end
end
return dereferenced_dobj_ids`

type MDSRedis struct {
	//*baseMeta
	Rdb          redis.UniversalClient
	prefix       string //DB name
	bucketPrefix string //BUK
	ObjectPrefic string //OBJ
	shaLookup    string // The SHA returned by Redis for the loaded `scriptLookup`
	shaResolve   string // The SHA returned by Redis for the loaded `scriptResolve`
	shaAddRef    string
	shaRemoveRef string
	metesetting  Format
}

//var _ Meta = (*MDSRedis)(nil)
//var _ engine = (*MDSRedis)(nil)

// newRedisMeta return a meta store using Redis.
func NewRedisMeta(driver, addr string, conf *Config) (MDS, error) {
	if driver != "redis" {
		return nil, fmt.Errorf("unsupported meta driver: %s", driver)
	}

	rdb, err := newUniversalRedisClient(addr, conf)
	if err != nil {
		return nil, err
	}

	// The DB number is already handled by the client, but we need a prefix for non-cluster keys.
	// In cluster mode, keys with hashtags `{...}` are placed on the same node.
	// We use the DB number to construct a prefix to simulate DBs in cluster mode.
	tmpOpt, _ := redis.ParseURL("redis://" + addr)
	prefix := fmt.Sprintf("DB%d", tmpOpt.DB)

	m := MDSRedis{
		Rdb:          rdb,
		prefix:       prefix,
		bucketPrefix: prefix + "BUK",
		ObjectPrefic: prefix + "OBJ",
	}
	//m.en = m
	m.checkServerConfig()
	if err := m.loadScripts(); err != nil {
		return nil, err
	}
	m.Init(&m.metesetting, true)
	return &m, nil
}

func (m *MDSRedis) loadScripts() (err error) {
	ctx := context.Background()
	m.shaAddRef, err = m.Rdb.ScriptLoad(ctx, addRefScript).Result()
	if err != nil {
		logger.Errorf("failed to load addRefScript: %v", err)
		return err
	}
	m.shaRemoveRef, err = m.Rdb.ScriptLoad(ctx, removeRefScript).Result()
	if err != nil {
		logger.Errorf("failed to load removeRefScript: %v", err)
		return err
	}
	logger.Info("Successfully loaded Redis Lua scripts for reference counting.")
	return nil
}

func (m *MDSRedis) checkServerConfig() {
	rawInfo, err := m.Rdb.Info(context.Background()).Result()
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
		if _, err := m.Rdb.ConfigSet(context.Background(), "maxmemory-policy", "noeviction").Result(); err != nil {
			logger.Errorf("try to reconfigure maxmemory-policy to 'noeviction' failed: %s", err)
		} else if result, err := m.Rdb.ConfigGet(context.Background(), "maxmemory-policy").Result(); err != nil {
			logger.Warnf("get config maxmemory-policy failed: %s", err)
		} else if len(result) == 1 && result["maxmemory-policy"] != "noeviction" {
			logger.Warnf("reconfigured maxmemory-policy to 'noeviction', but it's still %s", result["maxmemory-policy"])
		} else {
			logger.Infof("set maxmemory-policy to 'noeviction' successfully")
		}
	}
	start := time.Now()
	_, err = m.Rdb.Ping(context.Background()).Result()
	if err != nil {
		logger.Errorf("Ping redis: %s", err.Error())
		return
	}
	logger.Infof("Ping redis latency: %s", time.Since(start))
}

func (m *MDSRedis) Shutdown() error {
	return m.Rdb.Close()
}

func (m *MDSRedis) Name() string {
	return "redis"
}

func (m *MDSRedis) setting() string {
	return m.prefix + "setting"
}

func (m *MDSRedis) Init(format *Format, force bool) error {
	ctx := context.Background()
	body, err := m.Rdb.Get(ctx, m.setting()).Bytes()
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
		if err = m.Rdb.Set(ctx, m.setting(), jsonDataIndent, 0).Err(); err != nil {
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
	exists, err := m.Rdb.HExists(ctx, BucketsKey, bucket).Result()
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
	err = m.Rdb.HSet(ctx, BucketsKey, bucket, jsonData).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HSet bucket[%s]: %s", bucket, err)
		return err
	}
	return nil
}

func (m *MDSRedis) DelBucket(bucket string) error {
	ctx := context.Background()

	objCount, err := m.Rdb.HLen(ctx, bucket).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to HLen bucket[%s]: %s", bucket, err)
		return err
	}

	if objCount > 0 {
		logger.Errorf("the bucket:%s is not empty(%d objects left)", bucket, objCount)
		return fmt.Errorf("the bucket:%s is not empty(%d objects left)", bucket, objCount)
	}

	err = m.Rdb.Del(ctx, bucket).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to DelBucket[%s]: %s", bucket, err)
		return err
	}

	err = m.Rdb.HDel(ctx, BucketsKey, bucket).Err()
	if err != nil {
		logger.Errorf("MDSRedis::failed to DelBucket[%s] %s", bucket, err)
		return err
	}

	logger.Tracef("MDSRedis::successfully DelBucket[%s]", bucket)
	return nil
}

func (m *MDSRedis) ListBuckets() ([]minio.BucketInfo, error) {
	ctx := context.Background()
	buckets, err := m.Rdb.HGetAll(ctx, BucketsKey).Result()
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
	objects, err := m.Rdb.HGetAll(ctx, bucket).Result()
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

func (m *MDSRedis) PutObjectMeta(object minio.ObjectInfo, uniqueDOidlist []uint64) error {
	ctx := context.Background()
	ns, _, err := ParseNamespaceAndBucket(object.Bucket)
	if err != nil {
		logger.Errorf("failed to parse namespace and bucket: %s", err)
		return err
	}
	manifestID, ok := object.UserDefined[ManifestIDKey]
	if !ok || manifestID == "" {
		return fmt.Errorf("manifest ID not found in object metadata for object %s/%s", object.Bucket, object.Name)
	}

	//write reference
	err = m.AddReference(ns, uniqueDOidlist, object.Name)
	if err != nil {
		return err
	}
	//write object
	jsondata, err := json.Marshal(object)
	if err != nil {
		//cleanup the reference
		m.RemoveReference(ns, uniqueDOidlist, object.Name)
		return err
	}
	err = m.Rdb.HSet(ctx, object.Bucket, object.Name, jsondata).Err()
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
		return objInfo, err
	}
	return objInfo, nil
}

func (m *MDSRedis) GetObjectMeta(object *minio.ObjectInfo) error {
	ctx := context.Background()
	logger.Tracef("GetObjectMeta:objectKey=%s", object.Name)

	obj_info, err := m.Rdb.HGet(ctx, object.Bucket, object.Name).Result()
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
// This function now only deletes the object's primary metadata entry.
// Reference counting and manifest deletion are handled at the xlator level.
func (m *MDSRedis) DelObjectMeta(bucket string, obj string) error {
	ctx := context.Background()
	err := m.Rdb.HDel(ctx, bucket, obj).Err()
	if err != nil {
		logger.Errorf("failed to delete object[%s] meta from bucket %s, err:%s", obj, bucket, err)
		return err
	}
	return nil
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

	exists, err := m.Rdb.HExists(ctx, BucketsKey, bucket).Result()
	if err != nil {
		logger.Errorf("MakeBucket:failed to check exist for bucket:%s, err:%s", bucket, err)
		return false, err
	}

	return exists, nil
}

func (m *MDSRedis) GetIncreasedDCID() (int64, error) {
	ctx := context.Background()

	id, err := m.Rdb.Incr(ctx, dcidkey).Result()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (m *MDSRedis) GetIncreasedManifestID() (string, error) {
	ctx := context.Background()

	id, err := m.Rdb.Incr(ctx, manifestKey).Result()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Manifest%d", id), nil
}

func (m *MDSRedis) WriteManifest(manifestid string, manifestList []ChunkInManifest) error {
	ctx := context.Background()
	if len(manifestList) == 0 {
		return nil
	}

	serializedChunks := make([]interface{}, len(manifestList))
	for i, chunk := range manifestList {
		str, err := internal.SerializeToString(chunk)
		if err != nil {
			logger.Errorf("MDSRedis::failed to SerializeToString chunk[%v] : %s", chunk, err)
			return err
		}
		serializedChunks[i] = str
	}

	_, err := m.Rdb.RPush(ctx, manifestid, serializedChunks...).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to RPush chunks into manifest[%s] : %s", manifestid, err)
	}
	return err
}

func (m *MDSRedis) writeManifestReturnDCIDList(manifestid string, manifestList []ChunkInManifest) (dcidSet *internal.UInt64Set, err error) {
	ctx := context.Background()
	dcidSet = internal.NewUInt64Set()
	if len(manifestList) == 0 {
		return dcidSet, nil
	}

	serializedChunks := make([]interface{}, len(manifestList))
	for i, chunk := range manifestList {
		dcidSet.Add(chunk.DCID)
		str, err := internal.SerializeToString(chunk)
		if err != nil {
			logger.Errorf("MDSRedis::failed to SerializeToString chunk[%v] : %s", chunk, err)
			return nil, err
		}
		serializedChunks[i] = str
	}
	_, err = m.Rdb.RPush(ctx, manifestid, serializedChunks...).Result()
	if err != nil {
		logger.Errorf("MDSRedis::failed to RPush chunks into manifest[%s] : %s", manifestid, err)
		return nil, err
	}
	return dcidSet, nil
}

func (m *MDSRedis) delManifest(ctx context.Context, mfid string) (err error) {
	//delete minifest
	err = m.Rdb.Del(ctx, mfid).Err()
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
	manifestid, ok := objInfo.UserDefined[ManifestIDKey]
	if !ok || manifestid == "" {
		return nil, fmt.Errorf("manifest ID not found in object metadata for object %s/%s", bucket, object)
	}
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
	fps, err := m.Rdb.LRange(ctx, manifestid, 0, -1).Result()
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

func (m *MDSRedis) getManifestAndDCIDSet(manifestid string) (chunks []ChunkInManifest, dcidSet *internal.UInt64Set, err error) {
	ctx := context.Background()
	dcidSet = internal.NewUInt64Set()
	// Get the list of chunk IDs from the manifest
	fps, err := m.Rdb.LRange(ctx, manifestid, 0, -1).Result()
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
		dcidSet.Add(chunk.DCID)
	}
	return chunks, dcidSet, nil
}

// AddReference adds an object reference to multiple Data Objects in a batch.
// It uses a Redis Lua script to ensure atomicity and avoid WATCH-based transaction failures.
func (m *MDSRedis) AddReference(namespace string, dataContainerIDs []uint64, objectName string) error {
	ctx := context.Background()
	if len(dataContainerIDs) == 0 {
		return nil
	}
	refKey := GetRefKey(namespace)

	args := make([]interface{}, 0, len(dataContainerIDs)+1)
	args = append(args, objectName)
	for _, id := range dataContainerIDs {
		args = append(args, strconv.FormatUint(id, 10))
	}

	err := m.Rdb.EvalSha(ctx, m.shaAddRef, []string{refKey}, args...).Err()
	if err != nil {
		logger.Errorf("AddReference: failed to execute addRefScript for objName %s: %v", objectName, err)
		return err
	}
	return nil
}

// RemoveReference removes an object reference from multiple Data Objects in a batch.
// It uses a Redis Lua script to ensure atomicity and avoid WATCH-based transaction failures.
// The script returns a list of DCIDs that have become dereferenced.
func (m *MDSRedis) RemoveReference(namespace string, dataContainerIDs []uint64, objectName string) (dereferencedDCIDs []uint64, err error) {
	ctx := context.Background()
	if len(dataContainerIDs) == 0 {
		return nil, nil
	}
	refKey := GetRefKey(namespace)

	args := make([]interface{}, 0, len(dataContainerIDs)+1)
	args = append(args, objectName)
	for _, id := range dataContainerIDs {
		args = append(args, strconv.FormatUint(id, 10))
	}

	result, err := m.Rdb.EvalSha(ctx, m.shaRemoveRef, []string{refKey}, args...).Result()
	if err != nil {
		// The error message from the user indicates this is where the failure occurs.
		// The new error message will be more specific.
		logger.Errorf("RemoveReference: failed to execute removeRefScript for objName %s: %v", objectName, err)
		return nil, err
	}

	// The Lua script returns a table (slice in Go) of dereferenced DCID strings.
	if dereferencedIDs, ok := result.([]interface{}); ok {
		dereferencedDCIDs = make([]uint64, 0, len(dereferencedIDs))
		for _, idVal := range dereferencedIDs {
			if idStr, ok := idVal.(string); ok {
				id, err := strconv.ParseUint(idStr, 10, 64)
				if err != nil {
					logger.Warnf("RemoveReference: could not parse dereferenced DCID '%s' from script result: %v", idStr, err)
					continue
				}
				dereferencedDCIDs = append(dereferencedDCIDs, id)
			}
		}
	}

	return dereferencedDCIDs, nil
}

// AddDeletedDCIDs adds a list of Data Container IDs to the set of deleted DCIDs for a given namespace.
// This set is used by a background garbage collection process.
func (m *MDSRedis) AddDeletedDCIDs(namespace string, dcids []uint64) error {
	if len(dcids) == 0 {
		return nil
	}

	ctx := context.Background()
	key := GetDeletedDCIDKey(namespace)

	// Convert []uint64 to []interface{} for SAdd
	members := make([]interface{}, len(dcids))
	for i, dcid := range dcids {
		members[i] = dcid
	}

	// SAdd is atomic, so a transaction is not needed. Using WATCH here can cause
	// race conditions with the GC process that uses SRem on the same key,
	// leading to unnecessary transaction failures.
	err := m.Rdb.SAdd(ctx, key, members...).Err()
	if err != nil {
		logger.Errorf("AddDeletedDCIDs: failed to add DCIDs to set %s: %v", key, err)
		return err
	}
	logger.Tracef("AddDeletedDCIDs: added %d DCIDs to set %s", len(dcids), key)

	return nil
}

// GetAllNamespaces retrieves a list of all unique namespaces by inspecting all bucket names.
func (m *MDSRedis) GetAllNamespaces() ([]string, error) {
	ctx := context.Background()
	buckets, err := m.Rdb.HKeys(ctx, BucketsKey).Result()
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

// IsDCIDDeleted checks if a specific DCID is present in the deleted DCID set for a given namespace.
// It returns true if the DCID is marked for deletion, false otherwise.
func (m *MDSRedis) IsDCIDDeleted(namespace string, dcid uint64) (bool, error) {
	ctx := context.Background()
	key := GetDeletedDCIDKey(namespace)
	dcidStr := strconv.FormatUint(dcid, 10)

	isMember, err := m.Rdb.SIsMember(ctx, key, dcidStr).Result()
	if err != nil {
		logger.Errorf("IsDCIDDeleted: failed to check SISMEMBER for DCID %d in set %s: %v", dcid, key, err)
		return false, err
	}

	return isMember, nil
}

// GetRandomDeletedDCIDs retrieves a specified number of random DCIDs from the deleted set without removing them.
func (m *MDSRedis) GetRandomDeletedDCIDs(namespace string, count int64) ([]uint64, error) {
	if count <= 0 {
		return nil, nil
	}
	ctx := context.Background()
	key := GetDeletedDCIDKey(namespace)

	dcidStrs, err := m.Rdb.SRandMemberN(ctx, key, count).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Set is empty
		}
		logger.Errorf("GetRandomDeletedDCIDs: failed to SRANDMEMBER from %s: %v", key, err)
		return nil, err
	}

	if len(dcidStrs) == 0 {
		return nil, nil
	}

	dcids := make([]uint64, 0, len(dcidStrs))
	for _, s := range dcidStrs {
		dcid, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			logger.Warnf("GetRandomDeletedDCIDs: failed to parse DCID string '%s', skipping: %v", s, err)
			continue
		}
		dcids = append(dcids, dcid)
	}

	return dcids, nil
}

// RemoveSpecificDeletedDCIDs removes a list of specific DCIDs from the deleted set.
func (m *MDSRedis) RemoveSpecificDeletedDCIDs(namespace string, dcids []uint64) error {
	if len(dcids) == 0 {
		return nil
	}

	ctx := context.Background()
	key := GetDeletedDCIDKey(namespace)

	// Convert []uint64 to []interface{} for SRem
	members := make([]interface{}, len(dcids))
	for i, dcid := range dcids {
		members[i] = dcid
	}

	err := m.Rdb.SRem(ctx, key, members...).Err()
	if err != nil {
		logger.Errorf("RemoveSpecificDeletedDCIDs: failed to remove DCIDs from set %s: %v", key, err)
		return err
	}
	logger.Tracef("RemoveSpecificDeletedDCIDs: removed %d DCIDs from set %s", len(dcids), key)

	return nil
}
