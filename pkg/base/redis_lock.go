// Copyright 2024 zhengshuai.xiao@outlook.com
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
package base

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio/cmd"
	"github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

var (
	logger = internal.GetLogger("base")
)

const (
	// lockExpiry is the default duration for which a lock is held before it expires.
	// It must be long enough for the operation to complete.
	lockExpiry = 30 * time.Second

	// renewalInterval is the interval at which the lock's expiry is renewed.
	// It should be significantly shorter than lockExpiry.
	renewalInterval = 10 * time.Second

	// lockRetryInterval is the time to wait before retrying to acquire a lock.
	lockRetryInterval = 100 * time.Millisecond
)

// Lua script to release a write lock.
// It checks if the lock is held by the current owner before deleting it.
// KEYS[1]: The lock key
// ARGV[1]: The owner ID
const releaseWriteLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

// Lua script to release a read lock.
// It removes the owner ID from the set of read lock holders.
// KEYS[1]: The read lock key (a set)
// ARGV[1]: The owner ID
const releaseReadLockScript = `
return redis.call("srem", KEYS[1], ARGV[1])
`

// Lua script to renew a write lock.
// It checks if the lock is held by the current owner before extending its expiry.
// KEYS[1]: The lock key
// ARGV[1]: The owner ID
// ARGV[2]: The new expiry in milliseconds
const renewWriteLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
`

// RedisLock implements the minio.RWLocker interface using Redis.
type RedisLock struct {
	key        string // The key for the write lock in Redis.
	readKey    string // The key for the read lock set in Redis.
	ownerID    string // A unique ID for this lock instance.
	rdb        redis.UniversalClient
	cancelFunc context.CancelFunc // To stop the background renewal goroutine.
}

// newRedisLock creates a new distributed lock instance for a specific resource.
func NewRedisLock(rdb redis.UniversalClient, bucket string, objects ...string) *RedisLock {
	// Create a consistent key from the bucket and sorted object names.
	if len(objects) == 0 {
		return nil
	}
	if len(objects) > 1 {
		logger.Warnf("there is more than one objects, may have conflict, please avoid it")
	}
	sort.Strings(objects)
	resource := fmt.Sprintf("%s:%s", bucket, strings.Join(objects, ","))

	return &RedisLock{
		key:     fmt.Sprintf("lock:write:%s", resource),
		readKey: fmt.Sprintf("lock:read:%s", resource),
		ownerID: uuid.NewString(),
		rdb:     rdb,
	}
}

// GetLock attempts to acquire a write lock. It blocks until the lock is acquired or the context is cancelled.
func (l *RedisLock) GetLock(ctx context.Context, timeout *minio.DynamicTimeout) (context.Context, error) {
	logger.Tracef("Attempting to acquire write lock for key: %s", l.key)
	return l.acquireLock(ctx, timeout, false)
}

// GetRLock attempts to acquire a read lock. It blocks until the lock is acquired or the context is cancelled.
func (l *RedisLock) GetRLock(ctx context.Context, timeout *minio.DynamicTimeout) (context.Context, error) {
	logger.Tracef("Attempting to acquire read lock for key: %s", l.readKey)
	return l.acquireLock(ctx, timeout, true)
}

func (l *RedisLock) acquireLock(ctx context.Context, timeout *minio.DynamicTimeout, isReadLock bool) (context.Context, error) {
	deadline := time.Now().Add(timeout.Timeout())

	for time.Now().Before(deadline) {
		var acquired bool
		var err error

		if isReadLock {
			// For a read lock, we need to ensure no write lock exists.
			// This is not perfectly atomic without a more complex Lua script, but it's a common approach.
			// A better way would be a script that checks for write lock and adds to read lock atomically.
			if l.rdb.Exists(ctx, l.key).Val() == 0 {
				// No write lock, try to add ourselves to the read lock set.
				err = l.rdb.SAdd(ctx, l.readKey, l.ownerID).Err()
				if err == nil {
					l.rdb.Expire(ctx, l.readKey, lockExpiry) // Set expiry on the read lock set
					acquired = true
				}
			}
		} else {
			// For a write lock, try to set the key if it doesn't exist.
			acquired, err = l.rdb.SetNX(ctx, l.key, l.ownerID, lockExpiry).Result()
		}

		if err != nil {
			logger.Errorf("Error acquiring lock for key %s: %v", l.key, err)
			return nil, err
		}

		if acquired {
			// Lock acquired, start background renewal.
			var renewalCtx context.Context
			renewalCtx, l.cancelFunc = context.WithCancel(context.Background())
			go l.renew(renewalCtx, isReadLock)
			logger.Tracef("Successfully acquired lock for key: %s", l.key)
			return ctx, nil
		}

		// Lock not acquired, wait and retry.
		select {
		case <-time.After(lockRetryInterval):
			// continue loop
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, minio.OperationTimedOut{}
}

// renew runs in a background goroutine to periodically extend the lock's TTL.
func (l *RedisLock) renew(ctx context.Context, isReadLock bool) {
	ticker := time.NewTicker(renewalInterval)
	defer ticker.Stop()

	key := l.key
	if isReadLock {
		key = l.readKey
	}

	for {
		select {
		case <-ctx.Done():
			// The lock was released, or the context was cancelled.
			logger.Tracef("Stopping renewal for lock: %s", key)
			return
		case <-ticker.C:
			// Extend the lock's expiration.
			// Use a script to ensure we only extend the lock if we still own it.
			var err error
			if isReadLock {
				// For read locks, just extend the expiry of the set if we are a member.
				// A script could make this check atomic.
				if l.rdb.SIsMember(ctx, key, l.ownerID).Val() {
					err = l.rdb.Expire(ctx, key, lockExpiry).Err()
				}
			} else {
				// Use a script for atomic check-and-renew.
				err = l.rdb.Eval(ctx, renewWriteLockScript, []string{key}, l.ownerID, lockExpiry.Milliseconds()).Err()
			}

			if err != nil {
				logger.Warnf("Failed to renew lock for key %s: %v. The lock may have expired or been lost.", key, err)
				// Stop trying to renew if there's an error.
				return
			}
			logger.Tracef("Successfully renewed lock for key: %s", key)
		}
	}
}

// Unlock releases the write lock.
func (l *RedisLock) Unlock() {
	if l.cancelFunc != nil {
		l.cancelFunc() // Stop the renewal goroutine.
	}
	// Use Lua script to release the lock atomically and safely.
	_, err := l.rdb.Eval(context.Background(), releaseWriteLockScript, []string{l.key}, l.ownerID).Result()
	if err != nil {
		logger.Errorf("Failed to release write lock for key %s: %v", l.key, err)
	} else {
		logger.Tracef("Successfully released write lock for key: %s", l.key)
	}
}

// RUnlock releases the read lock.
func (l *RedisLock) RUnlock() {
	if l.cancelFunc != nil {
		l.cancelFunc() // Stop the renewal goroutine.
	}
	// Use Lua script to release the lock atomically.
	_, err := l.rdb.Eval(context.Background(), releaseReadLockScript, []string{l.readKey}, l.ownerID).Result()
	if err != nil {
		logger.Errorf("Failed to release read lock for key %s: %v", l.readKey, err)
	} else {
		logger.Tracef("Successfully released read lock for key: %s", l.readKey)
	}
}
