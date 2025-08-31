package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

const (
	adminCommandChannel = "dedup:admin:commands"
	triggerGCCommand    = "TRIGGER_GC"
)

func gcTriggerCmd() *cli.Command {
	return &cli.Command{
		Name:  "gctrigger",
		Usage: "Trigger a garbage collection cycle in the Dedup xlator",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "redis-addr", Value: "localhost:6379", Usage: "Redis server address"},
			&cli.StringFlag{Name: "redis-password", EnvVars: []string{"REDIS_PASSWORD"}, Usage: "Redis password"},
			&cli.IntFlag{Name: "redis-db", Value: 0, Usage: "Redis database number"},
		},
		Action: func(c *cli.Context) error {
			opts := &redis.Options{
				Addr:     c.String("redis-addr"),
				Password: c.String("redis-password"),
				DB:       c.Int("redis-db"),
			}

			rdb := redis.NewClient(opts)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := rdb.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("unable to connect to Redis at %s: %w", c.String("redis-addr"), err)
			}
			fmt.Printf("Successfully connected to Redis at %s.\n", c.String("redis-addr"))

			if err := rdb.Publish(ctx, adminCommandChannel, triggerGCCommand).Err(); err != nil {
				return fmt.Errorf("failed to publish GC trigger command to channel '%s': %w", adminCommandChannel, err)
			}

			fmt.Printf("Successfully published '%s' command to channel '%s'.\n", triggerGCCommand, adminCommandChannel)
			fmt.Println("Please check the XlatorS service logs to monitor GC progress.")
			return nil
		},
	}
}
