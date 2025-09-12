package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"github.com/minio/minio-go/v7"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/pkg/mytar"
)

func listBackupCmd() *cli.Command {
	return &cli.Command{
		Name:  "list-backup",
		Usage: "List the contents of a backup image stored in S3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Source bucket name"},
			&cli.StringFlag{Name: "object-base-name", Required: true, Usage: "Base name of the backup objects (without .DATA or .HDR suffix)"},
		},
		Action: func(c *cli.Context) error {
			bucketName := c.String("bucket")
			baseName := c.String("object-base-name")

			// 1. Get S3 client
			client, err := newS3Client(c)
			if err != nil {
				return fmt.Errorf("client initialization failed: %w", err)
			}

			ctx := c.Context
			hdrObjectName := baseName + ".HDR"

			// 2. Download and parse the HDR manifest object
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

			// 3. Print the contents in a table format
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "Mode\tSize\tModified\tName")
			for _, fileMeta := range manifest.Files {
				fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", os.FileMode(fileMeta.Header.Mode).String(), fileMeta.Header.Size, fileMeta.Header.ModTime.Format("2006-01-02 15:04:05"), fileMeta.Header.Name)
			}
			return w.Flush()
		},
	}
}
