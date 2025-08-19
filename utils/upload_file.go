package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOConfig 存储 MinIO 连接配置
type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
}

// 初始化 MinIO 客户端
func newMinIOClient(config MinIOConfig) (*minio.Client, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("创建客户端失败: %w", err)
	}
	return client, nil
}

// 确保存储桶存在，不存在则创建
func ensureBucket(ctx context.Context, client *minio.Client, bucketName string) error {
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("检查桶存在性失败: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("创建桶失败: %w", err)
		}
		fmt.Printf("存储桶 %s 创建成功\n", bucketName)
	}
	return nil
}

// 上传本地文件到 MinIO（修正版）
func uploadLocalFile(ctx context.Context, client *minio.Client, bucketName, objectName, localFilePath string) (minio.UploadInfo, error) {
	// 获取本地文件信息
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 打开本地文件
	file, err := os.Open(localFilePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 设置上传选项（已移除错误的 ContentLength）
	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream", // 设置文件MIME类型
	}

	// 上传文件：size 参数通过 fileInfo.Size() 传递，而非通过 opts
	uploadInfo, err := client.PutObject(
		ctx,
		bucketName,
		objectName,
		file,
		fileInfo.Size(), // 这里指定内容长度
		opts,
	)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("上传文件失败: %w", err)
	}

	return uploadInfo, nil
}

// 上传字节数据到 MinIO（修正版）
func uploadBytes(ctx context.Context, client *minio.Client, bucketName, objectName string, data []byte) (minio.UploadInfo, error) {
	// 将字节切片转换为 Reader
	reader := io.NopCloser(bytes.NewReader(data))

	// 上传字节数据：size 参数为 len(data)
	uploadInfo, err := client.PutObject(
		ctx,
		bucketName,
		objectName,
		reader,
		int64(len(data)), // 这里指定内容长度
		minio.PutObjectOptions{
			ContentType: "text/plain", // 文本类型示例
		},
	)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("上传字节数据失败: %w", err)
	}

	return uploadInfo, nil
}

func main() {
	// 配置 MinIO 连接信息
	config := MinIOConfig{
		Endpoint:        "localhost:9000",
		AccessKeyID:     "minio",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
		BucketName:      "xzs2",
	}

	// 创建上下文（设置 30 秒超时）
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 初始化客户端
	client, err := newMinIOClient(config)
	if err != nil {
		fmt.Printf("初始化失败: %v\n", err)
		return
	}

	// 确保存储桶存在
	if err := ensureBucket(ctx, client, config.BucketName); err != nil {
		fmt.Printf("存储桶处理失败: %v\n", err)
		return
	}

	// 示例 1: 上传本地文件
	localFile := "/tmp/1.data" // 替换为你的本地文件
	objectName1 := "1.data"
	uploadInfo, err := uploadLocalFile(ctx, client, config.BucketName, objectName1, localFile)
	if err != nil {
		fmt.Printf("本地文件上传失败: %v\n", err)
	} else {
		fmt.Printf("本地文件上传成功 - 对象名: %s, 大小: %d 字节, ETag: %s\n",
			uploadInfo.Key, uploadInfo.Size, uploadInfo.ETag)
	}

	// 示例 2: 上传字节数据
	/*
		textData := []byte("这是一段测试文本数据")
		objectName2 := "uploads/test-data.txt"
		uploadInfo, err = uploadBytes(ctx, client, config.BucketName, objectName2, textData)
		if err != nil {
			fmt.Printf("字节数据上传失败: %v\n", err)
		} else {
			fmt.Printf("字节数据上传成功 - 对象名: %s, 大小: %d 字节, ETag: %s\n",
				uploadInfo.Key, uploadInfo.Size, uploadInfo.ETag)
		}*/
}
