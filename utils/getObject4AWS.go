package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
		return 0, fmt.Errorf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get object: %v", err)
	}
	defer resp.Body.Close()

	outFile, err := os.Create(localFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create local file: %v", err)
	}
	defer outFile.Close()

	bytesCopied, err := io.Copy(outFile, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to copy file to local: %v", err)
	}
	return bytesCopied, nil
}

func main() {
	//endpoint must contain http:// it is different from MinIO
	endpoint := flag.String("endpoint", "http://127.0.0.1:9000", "MinIO server endpoint")
	accessKey := flag.String("access-key", "minio", "MinIO access key")
	secretKey := flag.String("secret-key", "minioadmin", "MinIO secret key")
	//useSSL := flag.Bool("ssl", false, "Use SSL for connection")
	bucket := flag.String("bucket", "", "Target bucket name (required)")
	localFile := flag.String("local-file", "", "Path to local file to upload (required)")
	key := flag.String("object-name", "", "Name for the object in MinIO (optional, uses local filename if empty)")

	region := "us-east-1"

	// Parse command line flags
	flag.Parse()

	start := time.Now()
	size, err := getObjectFromAWS(*endpoint, *accessKey, *secretKey, region, *bucket, *key, *localFile)
	if err != nil {
		log.Fatalf("Failed to download object: %v", err)
	}
	elapsed := time.Since(start)
	throughput := float64(size) / (1024 * 1024) / elapsed.Seconds()

	fmt.Printf(" %s/%s has been downloaded to %s successfully\n", *bucket, *key, *localFile)
	fmt.Printf("  Size:       %d bytes\n", size)
	fmt.Printf("  Time taken: %s\n", elapsed)
	fmt.Printf("  Throughput: %.2f MB/s\n", throughput)
}
