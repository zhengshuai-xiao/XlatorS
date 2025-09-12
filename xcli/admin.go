package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

func gcTriggerCmd() *cli.Command {
	return &cli.Command{
		Name:  "gctrigger",
		Usage: "Trigger a garbage collection cycle",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "redis-addr", Value: "localhost:6379/0", Usage: "Redis address (e.g., localhost:6379/0)"},
		},
		Action: func(c *cli.Context) error {
			redisAddr := c.String("redis-addr")
			// Use redis.ParseURL to correctly handle addresses with DB numbers.
			// It expects a URL format, so we prepend "redis://".
			opt, err := redis.ParseURL("redis://" + redisAddr)
			if err != nil {
				return fmt.Errorf("invalid redis address format '%s': %w", redisAddr, err)
			}
			rdb := redis.NewClient(opt)
			_, err = rdb.Publish(context.Background(), "dedup:admin:commands", "TRIGGER_GC").Result()
			if err == nil {
				log.Println("Successfully published GC trigger command.")
			}
			return err
		},
	}
}
