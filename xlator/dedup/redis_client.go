package dedup

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// newUniversalRedisClient creates a new Redis client that can connect to a single node,
// a cluster, or a sentinel setup.
// It centralizes the connection logic.
// NOTE: This function assumes the 'Config' struct (defined elsewhere in the package)
// contains the following fields for full functionality:
//
//	Retries               int
//	ReadTimeout           time.Duration
//	WriteTimeout          time.Duration
//
// TODO:TLS
func newUniversalRedisClient(addr string, conf *Config) (redis.UniversalClient, error) {
	uri := "redis://" + addr
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid redis address format: %w", err)
	}

	// Basic options from URL
	opt, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, fmt.Errorf("could not parse redis URL: %w", err)
	}

	// Password from environment if not in URL
	if opt.Password == "" {
		opt.Password = os.Getenv("REDIS_PASSWORD")
	}
	if opt.Password == "" {
		opt.Password = os.Getenv("META_PASSWORD")
	}

	// Universal client options
	universalOptions := &redis.UniversalOptions{
		Addrs:        strings.Split(u.Host, ","),
		DB:           opt.DB,
		Password:     opt.Password,
		MaxRetries:   conf.Retries,
		PoolSize:     100, // A sensible default
		ReadTimeout:  conf.ReadTimeout,
		WriteTimeout: conf.WriteTimeout,
	}

	if universalOptions.MaxRetries == 0 {
		universalOptions.MaxRetries = -1 // Disable retries for redis client
	}

	// Check for sentinel mode. Convention: masterName,sentinel1:port,sentinel2:port...
	hosts := strings.Split(u.Host, ",")
	if len(hosts) > 1 && !strings.Contains(hosts[0], ":") {
		universalOptions.MasterName = hosts[0]
		universalOptions.Addrs = hosts[1:]
		logger.Infof("Connecting to Redis in Sentinel mode. Master: %s, Sentinels: %v", universalOptions.MasterName, universalOptions.Addrs)
	} else if len(hosts) > 1 {
		logger.Infof("Connecting to Redis in Cluster mode. Nodes: %v", universalOptions.Addrs)
	} else {
		logger.Infof("Connecting to Redis in Single-node mode. Address: %s", universalOptions.Addrs[0])
	}

	rdb := redis.NewUniversalClient(universalOptions)

	// Ping to verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", addr, err)
	}

	logger.Info("Successfully connected to Redis.")
	return rdb, nil
}
