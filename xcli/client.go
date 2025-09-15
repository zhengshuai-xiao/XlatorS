package main

import (
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

func newS3Client(c *cli.Context) (*miniogo.Client, error) {
	endpoint := c.String("endpoint")
	accessKeyID := c.String("access-key")
	secretAccessKey := c.String("secret-key")
	useSSL := !c.Bool("no-ssl")

	minioClient, err := miniogo.New(endpoint, &miniogo.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}
