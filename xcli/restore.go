package main

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/pkg/mytar"
)

func restoreCmd() *cli.Command {
	return &cli.Command{
		Name:  "restore",
		Usage: "Restore a directory from a backup stored in S3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Source bucket name"},
			&cli.StringFlag{Name: "object-base-name", Required: true, Usage: "Base name of the backup objects (without .DATA or .HDR suffix)"},
			&cli.StringFlag{Name: "dest-dir", Required: true, Usage: "Destination directory to restore files to"},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			baseName := c.String("object-base-name")
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
			hdrObjectName := baseName + ".HDR"
			dataObjectName := baseName + ".DATA"

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

			// 5. Start a goroutine to reconstruct the tar stream
			go func() {
				defer pw.Close()
				log.Println("Reconstructing tar stream...")

				// Create a tar writer to write to the pipe
				tw := tar.NewWriter(pw)
				defer tw.Close()

				for _, fileMeta := range manifest.Files {
					// Write the original header
					if err := tw.WriteHeader(fileMeta.Header); err != nil {
						pw.CloseWithError(fmt.Errorf("failed to write tar header for %s: %w", fileMeta.Header.Name, err))
						return
					}

					// If it's a regular file with content, fetch its data from the .DATA object
					if fileMeta.Header.Typeflag == tar.TypeReg && fileMeta.Header.Size > 0 {
						// Get a reader for the specific data range
						rangeOpts := minio.GetObjectOptions{}
						if err := rangeOpts.SetRange(fileMeta.Offset, fileMeta.Offset+fileMeta.Header.Size-1); err != nil {
							pw.CloseWithError(fmt.Errorf("failed to set range for %s: %w", fileMeta.Header.Name, err))
							return
						}

						dataObj, err := client.GetObject(ctx, bucketName, dataObjectName, rangeOpts)
						if err != nil {
							pw.CloseWithError(fmt.Errorf("failed to get data for %s: %w", fileMeta.Header.Name, err))
							return
						}

						// Stream the data into the tar writer
						if _, err := io.Copy(tw, dataObj); err != nil {
							dataObj.Close()
							pw.CloseWithError(fmt.Errorf("failed to stream data for %s: %w", fileMeta.Header.Name, err))
							return
						}
						dataObj.Close()
					}
				}
				log.Println("Tar stream reconstruction complete.")
			}()

			// 6. Unpack the reconstructed tar stream from the pipe reader
			log.Printf("Unpacking archive to '%s'...", destDir)
			if err := mytar.Unpack(pr, destDir); err != nil {
				// Check if the error is from the pipe writer (our goroutine)
				if strings.Contains(err.Error(), "packing failed") || strings.Contains(err.Error(), "failed to") {
					return fmt.Errorf("error during tar stream reconstruction: %w", err)
				}
				return fmt.Errorf("failed to unpack tar stream: %w", err)
			}

			log.Printf("Restore completed successfully to directory '%s'.", destDir)
			return nil
		},
	}
}
