package S3client

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	miniogo "github.com/minio/minio-go/v7"
)

// 上传本地文件到 MinIO
func UploadFile(ctx context.Context, core *miniogo.Core, bucket, object, localFilePath string) (miniogo.UploadInfo, error) {
	// 1. 打开本地文件
	file, err := os.Open(localFilePath)
	if err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("failed to open file[%s]: %v", localFilePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("failed to stat file[%s]: %w", localFilePath, err)
	}
	fileSize := fileInfo.Size()

	md5Hash, sha256Hash, err := calculateFileHashes(file)
	if err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("failed to calc hash: %w", err)
	}

	if _, err := file.Seek(0, 0); err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("failed to reset file pointer: %w", err)
	}

	opts := miniogo.PutObjectOptions{
		ContentType: "application/octet-stream", // 默认二进制类型，可根据文件类型修改
		// 可添加其他选项，如：
		// StorageClass: "STANDARD_IA", // 存储类别
		// UserMetadata: map[string]string{"key": "value"}, // 自定义元数据
	}

	uploadInfo, err := core.PutObject(
		ctx,
		bucket,
		object,
		file,       // 文件作为 io.Reader
		fileSize,   // 文件大小
		md5Hash,    // MD5 哈希（base64 编码）
		sha256Hash, // SHA256 哈希（十六进制）
		opts,
	)
	if err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("failed to upload file[%s]: %w", localFilePath, err)
	}

	return uploadInfo, nil
}

func calculateFileHashes(file *os.File) (md5Base64 string, sha256Hex string, err error) {
	md5Hasher := md5.New()
	sha256Hasher := sha256.New()

	multiWriter := io.MultiWriter(md5Hasher, sha256Hasher)
	if _, err := io.Copy(multiWriter, file); err != nil {
		return "", "", err
	}

	md5Bytes := md5Hasher.Sum(nil)
	md5Base64 = base64.StdEncoding.EncodeToString(md5Bytes)

	sha256Bytes := sha256Hasher.Sum(nil)
	sha256Hex = hex.EncodeToString(sha256Bytes)

	return md5Base64, sha256Hex, nil
}
