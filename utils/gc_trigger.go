package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	adminCommandChannel = "dedup:admin:commands"
	triggerGCCommand    = "TRIGGER_GC"
)

func main() {
	// Define command-line flags for Redis connection
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis server address (e.g., localhost:6379)")
	redisPassword := flag.String("redis-password", "", "Redis password. Can also be set via REDIS_PASSWORD environment variable.")
	redisDB := flag.Int("redis-db", 0, "Redis database number to use.")

	flag.Parse()

	// Use environment variable for password if the flag is not provided
	if *redisPassword == "" {
		*redisPassword = os.Getenv("REDIS_PASSWORD")
	}

	// Create Redis client options
	opts := &redis.Options{
		Addr:     *redisAddr,
		Password: *redisPassword,
		DB:       *redisDB,
	}

	// Create a new Redis client
	rdb := redis.NewClient(opts)

	// Ping Redis to ensure the connection is alive
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Unable to connect to Redis at %s: %v\n", *redisAddr, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully connected to Redis at %s.\n", *redisAddr)

	// Publish the TRIGGER_GC command to the admin channel
	if err := rdb.Publish(ctx, adminCommandChannel, triggerGCCommand).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to publish GC trigger command to channel '%s': %v\n", adminCommandChannel, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully published '%s' command to channel '%s'.\n", triggerGCCommand, adminCommandChannel)
	fmt.Println("Please check the XlatorS service logs to monitor GC progress.")
}
