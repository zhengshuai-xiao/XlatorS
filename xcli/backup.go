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

//   - For size smaller than 128MiB PutObject automatically does a
//     single atomic Put operation.
//   - For size larger than 128MiB PutObject automatically does a
//     multipart Put operation.
//   - For size input as -1 PutObject does a multipart Put operation
//     until input stream reaches EOF. Maximum object size that can
//     be uploaded through this operation will be 5TiB.

// disable-multipart cannot be ture, because we do not know the data size
func backupCmd() *cli.Command {
	return &cli.Command{
		Name:  "backup",
		Usage: "Pack a directory and upload it as a tar stream to S3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Target bucket name"},
			&cli.StringFlag{Name: "src-dir", Required: true, Usage: "Source directory to back up"},
			//&cli.BoolFlag{Name: "disable-multipart", Usage: "Disable multipart upload"},
			&cli.StringFlag{Name: "partSize", Value: "1G", Usage: "Part size for multipart upload (e.g., 64M, 1G)"},
			&cli.StringFlag{Name: "chunk-method", Value: "FastCDC", Usage: "Chunking algorithm to use (FastCDC or FixedCDC)"},
		},
		Action: func(c *cli.Context) error {
			disableMultipart := false // always false
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

			partSize, err := parseSize(c.String("partSize"))
			if err != nil {
				return fmt.Errorf("invalid partSize: %w", err)
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
			uploadInfoChan := make(chan minio.UploadInfo, 2)

			start := time.Now()

			// Upload HDR object
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Uploading header object: %s", hdrObjectName)
				// The HDR object is typically small, so we can use simpler options.
				// Multipart is unlikely to be triggered.
				hdrOpts := minio.PutObjectOptions{
					ContentType:      "application/json",
					DisableMultipart: disableMultipart,
					PartSize:         partSize,
				}
				info, err := client.PutObject(ctx, bucketName, hdrObjectName, hReader, -1, hdrOpts)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload header object: %w", err)
				} else {
					uploadInfoChan <- info
				}
			}()

			// Upload DATA object
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Uploading data object: %s", dataObjectName)
				dataOpts := minio.PutObjectOptions{
					ContentType:      "application/octet-stream",
					DisableMultipart: disableMultipart,
					PartSize:         partSize,
					UserMetadata:     make(map[string]string),
				}
				dataOpts.UserMetadata["Chunk-Method"] = c.String("chunk-method")
				info, err := client.PutObject(ctx, bucketName, dataObjectName, dReader, -1, dataOpts)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload data object: %w", err)
				} else {
					uploadInfoChan <- info
				}
			}()

			// 7. Wait for uploads to complete and check for errors
			wg.Wait()
			elapsed := time.Since(start)
			close(errChan)
			close(uploadInfoChan)

			for uploadErr := range errChan {
				if uploadErr != nil {
					return uploadErr // Return the first error encountered
				}
			}

			var totalSize int64
			for info := range uploadInfoChan {
				totalSize += info.Size
			}

			var throughput float64
			if elapsed.Seconds() > 0 {
				throughput = float64(totalSize) / (1024 * 1024) / elapsed.Seconds()
			}

			log.Printf("Backup of '%s' completed successfully.", srcDir)
			log.Printf("  Header Object: s3://%s/%s", bucketName, hdrObjectName)
			log.Printf("  Data Object:   s3://%s/%s", bucketName, dataObjectName)
			log.Printf("  Total Size:    %d bytes", totalSize)
			log.Printf("  Time taken:    %s", elapsed)
			log.Printf("  Throughput:    %.2f MB/s", throughput)
			return nil
		},
	}
}
