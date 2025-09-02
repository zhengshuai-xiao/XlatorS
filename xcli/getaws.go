package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

func getAWSCmd() *cli.Command {
	return &cli.Command{
		Name:  "getaws",
		Usage: "Get an object from an S3-compatible backend using the AWS SDK",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "endpoint", Value: "http://127.0.0.1:9000", Usage: "S3 server endpoint (must include http:// or https://)"},
			&cli.StringFlag{Name: "access-key", Value: "minio", Usage: "S3 access key"},
			&cli.StringFlag{Name: "secret-key", Value: "minioadmin", Usage: "S3 secret key"},
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "Target bucket name"},
			&cli.StringFlag{Name: "object-name", Required: true, Usage: "Name for the object in S3"},
			&cli.StringFlag{Name: "local-file", Required: true, Usage: "Path to save the downloaded file"},
		},
		Action: func(c *cli.Context) error {
			region := "us-east-1" // This is often a default for S3-compatible stores

			start := time.Now()
			size, err := getObjectFromAWS(c.String("endpoint"), c.String("access-key"), c.String("secret-key"), region, c.String("bucket"), c.String("object-name"), c.String("local-file"))
			if err != nil {
				return fmt.Errorf("failed to download object: %w", err)
			}
			elapsed := time.Since(start)
			throughput := float64(size) / (1024 * 1024) / elapsed.Seconds()

			fmt.Printf(" %s/%s has been downloaded to %s successfully\n", c.String("bucket"), c.String("object-name"), c.String("local-file"))
			fmt.Printf("  Size:       %d bytes\n", size)
			fmt.Printf("  Time taken: %s\n", elapsed)
			fmt.Printf("  Throughput: %.2f MB/s\n", throughput)
			return nil
		},
	}
}

func getObjectFromAWS(endpoint, accessKey, secretKey, region, bucket, key, localFile string) (int64, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{
						URL:           endpoint,
						SigningRegion: region,
					}, nil
				}
				return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
			}),
		),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get object: %w", err)
	}
	defer resp.Body.Close()

	outFile, err := os.Create(localFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create local file: %w", err)
	}
	defer outFile.Close()

	return io.Copy(outFile, resp.Body)
}
