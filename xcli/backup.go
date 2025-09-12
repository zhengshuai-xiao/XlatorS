package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/pkg/mytar"
)

func backupCmd() *cli.Command {
	return &cli.Command{
		Name:  "backup",
		Usage: "Pack a directory and upload it as a tar stream to S3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Target bucket name"},
			&cli.StringFlag{Name: "src-dir", Required: true, Usage: "Source directory to back up"},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			srcDir := c.String("src-dir")

			// 1. Validate source directory
			info, err := os.Stat(srcDir)
			if err != nil {
				return fmt.Errorf("failed to stat source directory '%s': %w", srcDir, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("source path '%s' is not a directory", srcDir)
			}

			// 2. Get S3 client
			client, err := newS3Client(c)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			// 3. Ensure bucket exists
			ctx := c.Context
			exists, err := client.BucketExists(ctx, bucketName)
			if err != nil {
				return fmt.Errorf("failed to check bucket existence: %w", err)
			}
			if !exists {
				if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
					return fmt.Errorf("failed to create bucket: %w", err)
				}
				log.Printf("Bucket %s created successfully\n", bucketName)
			}

			// 4. Prepare object names and pipes
			timestamp := time.Now().Format("20060102150405")
			baseName := filepath.Base(srcDir)
			dataObjectName := fmt.Sprintf("%s-%s.DATA", baseName, timestamp)
			hdrObjectName := fmt.Sprintf("%s-%s.HDR", baseName, timestamp)

			hReader, hWriter := io.Pipe()
			dReader, dWriter := io.Pipe()

			// 5. Start the packing process in a goroutine
			go func() {
				defer hWriter.Close()
				defer dWriter.Close()

				log.Printf("Starting to pack directory '%s'...", srcDir)
				err := mytar.PackToWriters(srcDir, hWriter, dWriter)
				if err != nil {
					hWriter.CloseWithError(fmt.Errorf("packing failed: %w", err))
					dWriter.CloseWithError(fmt.Errorf("packing failed: %w", err))
				} else {
					log.Println("Packing completed successfully.")
				}
			}()

			// 6. Upload both streams concurrently
			var wg sync.WaitGroup
			errChan := make(chan error, 2)

			// Upload HDR object
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Uploading header object: %s", hdrObjectName)
				opts := minio.PutObjectOptions{ContentType: "application/json", PartSize: 500 * 1024 * 1024}
				_, err := client.PutObject(ctx, bucketName, hdrObjectName, hReader, -1, opts)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload header object: %w", err)
				}
			}()

			// Upload DATA object
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Uploading data object: %s", dataObjectName)
				opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
				_, err := client.PutObject(ctx, bucketName, dataObjectName, dReader, -1, opts)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload data object: %w", err)
				}
			}()

			// 7. Wait for uploads to complete and check for errors
			wg.Wait()
			close(errChan)

			for uploadErr := range errChan {
				if uploadErr != nil {
					return uploadErr // Return the first error encountered
				}
			}

			log.Printf("Backup of '%s' completed successfully.", srcDir)
			log.Printf("  Header Object: s3://%s/%s", bucketName, hdrObjectName)
			log.Printf("  Data Object:   s3://%s/%s", bucketName, dataObjectName)
			return nil
		},
	}
}
