package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/zhengshuai-xiao/S3Store/internal"
	"github.com/zhengshuai-xiao/S3Store/xlator/dedup"
)

func CalcFP(buf []byte, c *dedup.Chunk) {
	hash := sha256.New()
	hash.Write(buf[:])
	hashByte := hash.Sum(nil)
	//c.FP = hex.EncodeToString(hashByte)
	c.FP = hex.EncodeToString(hashByte)
	fmt.Printf("CalcFP:hashByte[%d]:\n%s,\n fp:%s", len(hashByte), hashByte, c.FP)
	//c.FP = hash.Sum(buf[c.off : c.off+c.len])
}

func CalcFPs(buf []byte, chunks []dedup.Chunk) {
	for i := range chunks {
		CalcFP(buf[:], &chunks[i])
	}
}

func readFile2Buf(localFilePath string) (data []byte, err error) {
	// Get local file information
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	fmt.Printf("filename:%s, file size:%d\n", localFilePath, fileInfo.Size())

	// Open local file
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 创建指定大小的缓冲区
	buf := make([]byte, 2048)

	for {
		// 读取数据到缓冲区
		n, err := file.Read(buf)
		if n > 0 {
			// 将读取到的内容追加到结果中
			data = append(data, buf[:n]...)
		}

		// 处理结束条件
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取文件失败: %w", err)
		}
	}
	fmt.Printf("readFile2Buf: read %d bytes\n", len(data))
	return data, nil
}

func main() {
	localFile := flag.String("file", "", "Path to local file (required)")

	// Parse command line flags
	flag.Parse()

	// Validate required parameters
	if *localFile == "" {
		fmt.Println("Error: local file path are required")
		flag.Usage()
		os.Exit(1)
	}

	cdc := dedup.FixedCDC{Chunksize: 128 * 1024}
	data, err := readFile2Buf(*localFile)
	if err != nil {
		return
	}
	chunks, err := cdc.Chunking(data)
	if err != nil {
		return
	}
	fmt.Printf("chunks:%v", chunks)

	fmt.Printf("data length %d bytes\n", len(data))

	dedup.CalcFPs(data, chunks)
	for _, chunk := range chunks {
		fmt.Printf("fp:%s \n", internal.StringToHex(chunk.FP))
	}
}
