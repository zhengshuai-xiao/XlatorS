package main

import (
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
)

func delBackupCmd() *cli.Command {
	return &cli.Command{
		Name:  "del-backup",
		Usage: "Delete a backup (both .DATA and .HDR objects) from S3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Source bucket name"},
			&cli.StringFlag{Name: "backup-id", Required: true, Usage: "The ID of the backup to delete"},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			backupID := c.String("backup-id")

			client, err := newS3Client(c)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			ctx := c.Context
			dataObjectName := backupID + ".DATA"
			hdrObjectName := backupID + ".HDR"

			objectsCh := make(chan minio.ObjectInfo)
			go func() {
				defer close(objectsCh)
				objectsCh <- minio.ObjectInfo{Key: dataObjectName}
				objectsCh <- minio.ObjectInfo{Key: hdrObjectName}
			}()

			for rErr := range client.RemoveObjects(ctx, bucketName, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
				return fmt.Errorf("failed to remove object %s: %w", rErr.ObjectName, rErr.Err)
			}

			log.Printf("Successfully deleted backup with ID: %s", backupID)
			return nil
		},
	}
}
