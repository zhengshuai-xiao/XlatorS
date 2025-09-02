package main

import (
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

func deleteCmd() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete an object from MinIO/XlatorS",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "endpoint", Value: "localhost:9000", Usage: "MinIO server endpoint"},
			&cli.StringFlag{Name: "access-key", Value: "minio", Usage: "MinIO access key"},
			&cli.StringFlag{Name: "secret-key", Value: "minioadmin", Usage: "MinIO secret key"},
			&cli.BoolFlag{Name: "ssl", Usage: "Use SSL for connection"},
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Target bucket name"},
			&cli.StringFlag{Name: "object-name", Required: true, Usage: "Name of the object to delete"},
		},
		Action: func(c *cli.Context) error {
			config := minio.Options{
				Creds:  credentials.NewStaticV4(c.String("access-key"), c.String("secret-key"), ""),
				Secure: c.Bool("ssl"),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			client, err := minio.New(c.String("endpoint"), &config)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			bucketName := c.String("bucket")
			objectName := c.String("object-name")

			opts := minio.RemoveObjectOptions{}
			err = client.RemoveObject(ctx, bucketName, objectName, opts)
			if err != nil {
				return fmt.Errorf("failed to delete object: %w", err)
			}

			fmt.Printf("Successfully deleted object '%s' from bucket '%s'.\n", objectName, bucketName)
			return nil
		},
	}
}
