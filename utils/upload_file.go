package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	G = 1 * 1024 * 1024 * 1024
)

// MinIOConfig holds configuration parameters for MinIO connection
type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

// newMinIOClient initializes a new MinIO client
func newMinIOClient(config MinIOConfig) (*minio.Client, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return client, nil
}

// ensureBucket checks if bucket exists, creates it if not
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

// uploadLocalFile uploads a local file to MinIO
func uploadLocalFile(ctx context.Context, client *minio.Client, bucketName, objectName, localFilePath string, disableMultipart bool, partSize uint64) (minio.UploadInfo, error) {
	// Get local file information
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to get file info: %w", err)
	}

	// Open local file
	file, err := os.Open(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Set upload options
	opts := minio.PutObjectOptions{
		ContentType:      "application/octet-stream",
		PartSize:         partSize,
		DisableMultipart: disableMultipart,
	}

	// Upload file
	uploadInfo, err := client.PutObject(
		ctx,
		bucketName,
		objectName,
		file,
		fileInfo.Size(),
		opts,
	)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to upload file: %w", err)
	}

	return uploadInfo, nil
}

func main() {
	// Define command line flags
	endpoint := flag.String("endpoint", "localhost:9000", "MinIO server endpoint")
	accessKey := flag.String("access-key", "minio", "MinIO access key")
	secretKey := flag.String("secret-key", "minioadmin", "MinIO secret key")
	useSSL := flag.Bool("ssl", false, "Use SSL for connection")
	bucketName := flag.String("bucket", "", "Target bucket name (required)")
	localFile := flag.String("local-file", "", "Path to local file to upload (required)")
	objectName := flag.String("object-name", "", "Name for the object in MinIO (optional, uses local filename if empty)")
	disableMultipart := flag.Bool("disable-multipart", false, "Disable multipart upload")
	partSize := flag.Uint64("partSize", G, "Disable multipart upload")

	// Parse command line flags
	flag.Parse()

	// Validate required parameters
	if *bucketName == "" || *localFile == "" {
		fmt.Println("Error: Both bucket name and local file path are required")
		flag.Usage()
		os.Exit(1)
	}

	// Use local filename as object name if not specified
	if *objectName == "" {
		_, fileName := filepath.Split(*localFile)
		*objectName = fileName
		fmt.Printf("Using local filename as object name: %s\n", *objectName)
	}

	// Create configuration
	config := MinIOConfig{
		Endpoint:        *endpoint,
		AccessKeyID:     *accessKey,
		SecretAccessKey: *secretKey,
		UseSSL:          *useSSL,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize client
	client, err := newMinIOClient(config)
	if err != nil {
		fmt.Printf("Client initialization failed: %v\n", err)
		os.Exit(1)
	}

	// Ensure bucket exists
	if err := ensureBucket(ctx, client, *bucketName); err != nil {
		fmt.Printf("Bucket processing failed: %v\n", err)
		os.Exit(1)
	}

	// Upload file
	uploadInfo, err := uploadLocalFile(ctx, client, *bucketName, *objectName, *localFile, *disableMultipart, *partSize)
	if err != nil {
		fmt.Printf("File upload failed: %v\n", err)
		os.Exit(1)
	}

	// Print success message
	fmt.Printf("File uploaded successfully:\n")
	fmt.Printf("  Bucket:     %s\n", *bucketName)
	fmt.Printf("  Object:     %s\n", uploadInfo.Key)
	fmt.Printf("  Size:       %d bytes\n", uploadInfo.Size)
	fmt.Printf("  ETag:       %s\n", uploadInfo.ETag)
}
