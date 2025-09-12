package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
)

const (
	G = 1 * 1024 * 1024 * 1024
)

func uploadCmd() *cli.Command {
	return &cli.Command{
		Name:  "upload",
		Usage: "Upload a local file to MinIO/XlatorS",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Target bucket name"},
			&cli.StringFlag{Name: "local-file", Required: true, Usage: "Path to local file to upload"},
			&cli.StringFlag{Name: "object-name", Usage: "Name for the object in MinIO (optional, uses local filename if empty)"},
			&cli.BoolFlag{Name: "disable-multipart", Usage: "Disable multipart upload"},
			&cli.Uint64Flag{Name: "partSize", Value: G, Usage: "Part size for multipart upload"},
			&cli.StringFlag{Name: "chunk-method", Value: "FastCDC", Usage: "Chunking algorithm to use (FastCDC or FixedCDC)"},
		},
		Action: func(c *cli.Context) error {
			objectName := c.String("object-name")
			localFile := c.String("local-file")
			if objectName == "" {
				_, fileName := filepath.Split(localFile)
				objectName = fileName
				fmt.Printf("Using local filename as object name: %s\n", objectName)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
			defer cancel()

			client, err := newS3Client(c)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			bucketName := c.String("bucket")
			if err := ensureBucket(ctx, client, bucketName); err != nil {
				return fmt.Errorf("bucket processing failed: %w", err)
			}

			start := time.Now()
			uploadInfo, err := uploadLocalFile(ctx, client, bucketName, objectName, localFile, c.Bool("disable-multipart"), c.Uint64("partSize"), c.String("chunk-method"))
			if err != nil {
				return fmt.Errorf("file upload failed: %w", err)
			}
			elapsed := time.Since(start)
			throughput := float64(uploadInfo.Size) / (1024 * 1024) / elapsed.Seconds()

			fmt.Printf("File uploaded successfully:\n")
			fmt.Printf("  Bucket:     %s\n", bucketName)
			fmt.Printf("  Object:     %s\n", uploadInfo.Key)
			fmt.Printf("  Size:       %d bytes\n", uploadInfo.Size)
			fmt.Printf("  ETag:       %s\n", uploadInfo.ETag)
			fmt.Printf("  Time taken: %s\n", elapsed)
			fmt.Printf("  Throughput: %.2f MB/s\n", throughput)
			return nil
		},
	}
}

func ensureBucket(ctx context.Context, client *minio.Client, bucketName string) error {
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		fmt.Printf("Bucket %s created successfully\n", bucketName)
	}
	return nil
}

func uploadLocalFile(ctx context.Context, client *minio.Client, bucketName, objectName, localFilePath string, disableMultipart bool, partSize uint64, chunkMethod string) (minio.UploadInfo, error) {
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to get file info: %w", err)
	}

	file, err := os.Open(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	opts := minio.PutObjectOptions{
		ContentType:      "application/octet-stream",
		PartSize:         partSize,
		DisableMultipart: disableMultipart,
		SendContentMd5:   true,
		UserMetadata:     make(map[string]string),
	}
	opts.UserMetadata["Chunk-Method"] = chunkMethod

	return client.PutObject(
		ctx,
		bucketName,
		objectName,
		file,
		fileInfo.Size(),
		opts,
	)
}

func downloadCmd() *cli.Command {
	return &cli.Command{
		Name:  "download", // Named to match test script
		Usage: "Download an object from a bucket",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true},
			&cli.StringFlag{Name: "object-name", Required: true},
			&cli.StringFlag{Name: "local-file", Required: true},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			objectName := c.String("object-name")
			localFile := c.String("local-file")

			s3Client, err := newS3Client(c)
			if err != nil {
				return err
			}

			start := time.Now()
			err = s3Client.FGetObject(context.Background(), bucketName, objectName, localFile, minio.GetObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to download object: %w", err)
			}
			elapsed := time.Since(start)

			fileInfo, err := os.Stat(localFile)
			if err != nil {
				return fmt.Errorf("failed to get info of downloaded file: %w", err)
			}

			var throughput float64
			if elapsed.Seconds() > 0 {
				throughput = float64(fileInfo.Size()) / (1024 * 1024) / elapsed.Seconds()
			}

			fmt.Printf("Object downloaded successfully:\n")
			fmt.Printf("  From:       s3://%s/%s\n", bucketName, objectName)
			fmt.Printf("  To:         %s\n", localFile)
			fmt.Printf("  Size:       %d bytes\n", fileInfo.Size())
			fmt.Printf("  Time taken: %s\n", elapsed)
			fmt.Printf("  Throughput: %.2f MB/s\n", throughput)
			return nil
		},
	}
}

func deleteCmd() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete an object from a bucket",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true},
			&cli.StringFlag{Name: "object-name", Required: true},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			objectName := c.String("object-name")

			s3Client, err := newS3Client(c)
			if err != nil {
				return err
			}
			err = s3Client.RemoveObject(context.Background(), bucketName, objectName, minio.RemoveObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete object: %w", err)
			}
			fmt.Printf("Successfully deleted object '%s' from bucket '%s'.\n", objectName, bucketName)
			return nil
		},
	}
}
