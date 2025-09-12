package main

import (
	"context"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
)

func makeBucketCmd() *cli.Command {
	return &cli.Command{
		Name:      "mb",
		Usage:     "Create a new bucket. Usage: xcli mb <bucket-name>",
		ArgsUsage: "BUCKET_NAME",
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return fmt.Errorf("bucket name is required")
			}
			bucketName := c.Args().First()

			s3Client, err := newS3Client(c)
			if err != nil {
				return err
			}

			err = s3Client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
			if err != nil {
				if minio.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
					log.Printf("Bucket '%s' already exists and is owned by you.", bucketName)
					return nil
				}
				return err
			}
			log.Printf("Successfully created bucket '%s'.", bucketName)
			return nil
		},
	}
}

func removeBucketCmd() *cli.Command {
	return &cli.Command{
		Name:      "rb",
		Usage:     "Remove a bucket. Usage: xcli rb <bucket-name>",
		ArgsUsage: "BUCKET_NAME",
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return fmt.Errorf("bucket name is required")
			}
			bucketName := c.Args().First()

			s3Client, err := newS3Client(c)
			if err != nil {
				return err
			}

			err = s3Client.RemoveBucket(context.Background(), bucketName)
			if err != nil {
				return err
			}
			log.Printf("Successfully removed bucket '%s'.", bucketName)
			return nil
		},
	}
}
