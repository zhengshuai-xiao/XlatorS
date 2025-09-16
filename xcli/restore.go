package main

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/pkg/mytar"
)

// reconstructTarStreamPartial handles the "partial restore" logic.
// It iterates through the manifest, and for each file/dir that needs to be restored,
// it fetches its content via an S3 Range GET request and writes it to the tar stream.
// This function starts a goroutine and returns a channel that will receive the total
// downloaded size upon completion.
func reconstructTarStreamPartial(
	ctx context.Context,
	client *minio.Client,
	bucketName, dataObjectName string,
	manifest *mytar.TarManifest,
	pathsToRestore []string,
	pw *io.PipeWriter,
) <-chan int64 {
	sizeChan := make(chan int64, 1)

	go func() {
		defer pw.Close()
		defer close(sizeChan)

		log.Println("Reconstructing tar stream via partial downloads...")

		tw := tar.NewWriter(pw)
		defer tw.Close()
		var downloadedSize int64

		for _, fileMeta := range manifest.Files {
			shouldRestore := false
			if len(pathsToRestore) == 0 {
				shouldRestore = true // Restore all if no specific paths are provided
			} else {
				for _, pathToRestore := range pathsToRestore {
					cleanPathToRestore := strings.TrimRight(pathToRestore, "/")
					cleanHeaderName := strings.TrimRight(fileMeta.Header.Name, "/")

					if cleanHeaderName == cleanPathToRestore || strings.HasPrefix(fileMeta.Header.Name, cleanPathToRestore+"/") ||
						(fileMeta.Header.Typeflag == tar.TypeDir && strings.HasPrefix(cleanPathToRestore, cleanHeaderName+"/")) {
						shouldRestore = true
						break
					}
				}
			}

			if !shouldRestore {
				continue
			}

			if err := tw.WriteHeader(fileMeta.Header); err != nil {
				pw.CloseWithError(fmt.Errorf("failed to write tar header for %s: %w", fileMeta.Header.Name, err))
				return
			}

			if fileMeta.Header.Typeflag == tar.TypeReg && fileMeta.Header.Size > 0 {
				if err := streamObjectRange(ctx, client, bucketName, dataObjectName, fileMeta, tw); err != nil {
					pw.CloseWithError(err)
					return
				}
				downloadedSize += fileMeta.Header.Size
			}
		}

		sizeChan <- downloadedSize
		log.Println("Tar stream reconstruction complete.")
	}()

	return sizeChan
}

// streamObjectRange fetches a specific byte range from an S3 object and streams it to a writer.
func streamObjectRange(ctx context.Context, client *minio.Client, bucket, object string, meta mytar.TarFileHeader, w io.Writer) error {
	rangeOpts := minio.GetObjectOptions{}
	if err := rangeOpts.SetRange(meta.Offset, meta.Offset+meta.Header.Size-1); err != nil {
		return fmt.Errorf("failed to set range for %s: %w", meta.Header.Name, err)
	}

	dataObj, err := client.GetObject(ctx, bucket, object, rangeOpts)
	if err != nil {
		return fmt.Errorf("failed to get data for %s: %w", meta.Header.Name, err)
	}
	defer dataObj.Close()

	if _, err := io.Copy(w, dataObj); err != nil {
		return fmt.Errorf("failed to stream data for %s: %w", meta.Header.Name, err)
	}
	return nil
}

// reconstructTarStreamWhole handles the "full restore" logic by streaming the entire .DATA object.
// It reads the .DATA object as a single stream and, for each file in the manifest,
// writes the tar header and then copies the corresponding number of bytes from the stream.
// This avoids random-access I/O and is highly efficient for full restores.
func reconstructTarStreamWhole(
	ctx context.Context,
	client *minio.Client,
	bucketName, dataObjectName string,
	manifest *mytar.TarManifest,
	pw *io.PipeWriter,
) <-chan int64 {
	sizeChan := make(chan int64, 1)

	go func() {
		defer pw.Close()
		defer close(sizeChan)

		log.Println("Reconstructing tar stream via whole object download...")

		dataObj, err := client.GetObject(ctx, bucketName, dataObjectName, minio.GetObjectOptions{})
		if err != nil {
			// If the object doesn't exist (e.g., empty backup), we can proceed gracefully.
			if minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
				log.Printf("Data object %s not found, likely an empty backup. Proceeding.", dataObjectName)
				sizeChan <- 0
				return
			}
			pw.CloseWithError(fmt.Errorf("failed to get data object %s: %w", dataObjectName, err))
			return
		}
		defer dataObj.Close()

		tw := tar.NewWriter(pw)
		defer tw.Close()

		var totalDataSize int64
		for _, fileMeta := range manifest.Files {
			if err := tw.WriteHeader(fileMeta.Header); err != nil {
				pw.CloseWithError(fmt.Errorf("failed to write tar header for %s: %w", fileMeta.Header.Name, err))
				return
			}

			if fileMeta.Header.Typeflag == tar.TypeReg && fileMeta.Header.Size > 0 {
				if _, err := io.CopyN(tw, dataObj, fileMeta.Header.Size); err != nil {
					pw.CloseWithError(fmt.Errorf("failed to stream data for %s: %w", fileMeta.Header.Name, err))
					return
				}
				totalDataSize += fileMeta.Header.Size
			}
		}
		sizeChan <- totalDataSize
		log.Println("Tar stream reconstruction complete.")
	}()

	return sizeChan
}

func restoreCmd() *cli.Command {
	return &cli.Command{
		Name:      "restore",
		Usage:     "Restore a directory or specific files from a backup stored in S3",
		ArgsUsage: "[files/dirs to restore...]",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Source bucket name"},
			&cli.StringFlag{Name: "backup-id", Required: true, Usage: "The ID of the backup to restore"},
			&cli.StringFlag{Name: "dest-dir", Required: true, Usage: "Destination directory to restore files to"},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			backupID := c.String("backup-id")
			destDir := c.String("dest-dir")

			// 1. Create destination directory if it doesn't exist
			if err := os.MkdirAll(destDir, 0755); err != nil {
				return fmt.Errorf("failed to create destination directory '%s': %w", destDir, err)
			}

			// 2. Get S3 client
			client, err := newS3Client(c)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			ctx := c.Context
			hdrObjectName := backupID + ".HDR"
			dataObjectName := backupID + ".DATA"

			// 3. Download and parse the HDR manifest object
			log.Printf("Downloading manifest: %s", hdrObjectName)
			hdrObj, err := client.GetObject(ctx, bucketName, hdrObjectName, minio.GetObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to download header object: %w", err)
			}
			defer hdrObj.Close()

			var manifest mytar.TarManifest
			if err := json.NewDecoder(hdrObj).Decode(&manifest); err != nil {
				return fmt.Errorf("failed to parse tar manifest: %w", err)
			}
			log.Printf("Manifest parsed successfully, found %d files/directories.", len(manifest.Files))

			// 4. Create a pipe to reconstruct the tar stream
			pr, pw := io.Pipe()

			start := time.Now()
			pathsToRestore := c.Args().Slice()
			isFullRestore := len(pathsToRestore) == 0

			// 5. Start a goroutine to reconstruct the tar stream
			var sizeChan <-chan int64
			if isFullRestore {
				sizeChan = reconstructTarStreamWhole(ctx, client, bucketName, dataObjectName, &manifest, pw)
			} else {
				sizeChan = reconstructTarStreamPartial(ctx, client, bucketName, dataObjectName, &manifest, pathsToRestore, pw)
			}

			// 6. Unpack the reconstructed tar stream from the pipe reader
			log.Printf("Unpacking archive to '%s'...", destDir)
			if err := mytar.Unpack(pr, destDir); err != nil {
				return fmt.Errorf("failed to unpack tar stream: %w", err)
			}

			// For full restore, totalDownloadedSize is the sum of all file contents.
			// For partial restore, it's the sum of downloaded ranges.
			totalDataSize := <-sizeChan
			elapsed := time.Since(start)
			var throughput float64
			if elapsed.Seconds() > 0 {
				throughput = float64(totalDataSize) / (1024 * 1024) / elapsed.Seconds()
			}

			log.Printf("Restore completed successfully to directory '%s'.", destDir)
			log.Printf("  Total Data Processed: %d bytes", totalDataSize)
			log.Printf("  Time taken:       %s", elapsed)
			log.Printf("  Avg Speed:        %.2f MB/s", throughput)
			return nil
		},
	}
}
