package leader_election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zhengshuai-xiao/XlatorS/internal" // Assuming logger is here
)

var logger = internal.GetLogger("LeaderElection")

// LeaderElector defines the interface for a leader election mechanism.
type LeaderElector interface {
	// Start begins the leader election process.
	// onAcquire is called when leadership is acquired, with a context that is cancelled when leadership is lost.
	// onRelease is called when leadership is lost.
	Start(ctx context.Context, onAcquire func(context.Context), onRelease func()) error
	// Stop gracefully stops the leader election process.
	Stop()
	// IsLeader returns true if this instance is currently the leader.
	IsLeader() bool
	// InstanceID returns the unique ID of this elector instance.
	InstanceID() string
}

// RedisLeaderElector implements LeaderElector using Redis.
type RedisLeaderElector struct {
	rdb               redis.UniversalClient
	leaderKey         string
	instanceID        string
	leaseDuration     time.Duration
	renewalInterval   time.Duration
	isLeader          bool
	stopChan          chan struct{}
	wg                sync.WaitGroup
	onAcquireCallback func(context.Context)
	onReleaseCallback func()
	leaderContext     context.Context
	leaderCancelFunc  context.CancelFunc
}

// NewRedisLeaderElector creates a new RedisLeaderElector instance.
func NewRedisLeaderElector(rdb redis.UniversalClient, leaderKey string, leaseDuration, renewalInterval time.Duration) *RedisLeaderElector {
	return &RedisLeaderElector{
		rdb:             rdb,
		leaderKey:       leaderKey,
		instanceID:      uuid.New().String(),
		leaseDuration:   leaseDuration,
		renewalInterval: renewalInterval,
		stopChan:        make(chan struct{}),
	}
}

// Start begins the leader election process.
// onAcquire is called when leadership is acquired, with a context that is cancelled when leadership is lost.
// onRelease is called when leadership is lost.
func (e *RedisLeaderElector) Start(ctx context.Context, onAcquire func(context.Context), onRelease func()) error {
	if e.onAcquireCallback != nil || e.onReleaseCallback != nil {
		return fmt.Errorf("leader elector already started or callbacks already set")
	}
	e.onAcquireCallback = onAcquire
	e.onReleaseCallback = onRelease

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		logger.Infof("Leader election worker for key '%s' (instance: %s) started.", e.leaderKey, e.instanceID)

		electionTicker := time.NewTicker(e.renewalInterval)
		defer electionTicker.Stop()

		for {
			select {
			case <-electionTicker.C:
				if e.isLeader {
					// Try to renew leadership
					if !e.renewLeadership() {
						logger.Warnf("Instance %s lost leadership for key '%s'.", e.instanceID, e.leaderKey)
						e.releaseLeadership()
					}
				} else {
					// Try to acquire leadership
					if e.tryAcquireLeadership() {
						logger.Infof("Instance %s acquired leadership for key '%s'.", e.instanceID, e.leaderKey)
						e.acquireLeadership()
					}
				}
			case <-e.stopChan:
				logger.Infof("Leader election worker for key '%s' (instance: %s) stopping.", e.leaderKey, e.instanceID)
				if e.isLeader {
					e.releaseLeadership() // Ensure leadership is released on graceful shutdown
				}
				return
			case <-ctx.Done(): // External context cancellation (e.g., application shutdown)
				logger.Infof("Leader election worker for key '%s' (instance: %s) stopping due to external context cancellation.", e.leaderKey, e.instanceID)
				if e.isLeader {
					e.releaseLeadership()
				}
				return
			}
		}
	}()
	return nil
}

// Stop gracefully stops the leader election process.
func (e *RedisLeaderElector) Stop() {
	close(e.stopChan)
	e.wg.Wait()
}

// IsLeader returns true if this instance is currently the leader.
func (e *RedisLeaderElector) IsLeader() bool {
	return e.isLeader
}

// InstanceID returns the unique ID of this elector instance.
func (e *RedisLeaderElector) InstanceID() string {
	return e.instanceID
}

// tryAcquireLeadership attempts to acquire the leader lock.
func (e *RedisLeaderElector) tryAcquireLeadership() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Short timeout for acquisition
	defer cancel()

	// SET key value NX PX expiry_ms
	cmd := e.rdb.SetNX(ctx, e.leaderKey, e.instanceID, e.leaseDuration)
	if cmd.Err() != nil {
		logger.Errorf("Failed to try acquire leadership lock for key '%s': %v", e.leaderKey, cmd.Err())
		return false
	}
	return cmd.Val() // Returns true if the key was set (i.e., lock acquired)
}

// renewLeadership attempts to renew the leader lock.
func (e *RedisLeaderElector) renewLeadership() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Short timeout for renewal
	defer cancel()

	// Use a Lua script for atomic check-and-renew: IF current value matches my instance ID, THEN renew TTL
	script := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("PEXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
    `
	cmd := e.rdb.Eval(ctx, script, []string{e.leaderKey}, e.instanceID, e.leaseDuration.Milliseconds())
	if cmd.Err() != nil {
		logger.Errorf("Failed to renew leadership lock for key '%s': %v", e.leaderKey, cmd.Err())
		return false
	}
	// Eval returns 1 on success for PEXPIRE, 0 if key not found or value mismatch.
	// We expect 1 for successful renewal.
	return cmd.Val().(int64) == 1
}

// acquireLeadership sets the internal state and calls the onAcquire callback.
func (e *RedisLeaderElector) acquireLeadership() {
	e.isLeader = true
	e.leaderContext, e.leaderCancelFunc = context.WithCancel(context.Background())
	if e.onAcquireCallback != nil {
		e.onAcquireCallback(e.leaderContext)
	}
}

// releaseLeadership sets the internal state and calls the onRelease callback.
func (e *RedisLeaderElector) releaseLeadership() {
	e.isLeader = false
	if e.leaderCancelFunc != nil {
		e.leaderCancelFunc() // Signal the running leader task to stop
		e.leaderCancelFunc = nil
	}
	if e.onReleaseCallback != nil {
		e.onReleaseCallback()
	}
}
