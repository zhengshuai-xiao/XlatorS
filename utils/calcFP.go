package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/xlator/dedup"
)

func main() {
	localFile := flag.String("file", "", "Path to local file (required)")
	avgSize := flag.Int("avg", 128*1024, "Average chunk size for CDC")
	minSize := flag.Int("min", 64*1024, "Min chunk size for FastCDC")
	maxSize := flag.Int("max", 256*1024, "Max chunk size for FastCDC")
	useFastCDC := flag.Bool("fastcdc", false, "Use FastCDC instead of FixedCDC")

	// Parse command line flags
	flag.Parse()

	// Validate required parameters
	if *localFile == "" {
		fmt.Println("Error: local file path is required")
		flag.Usage()
		os.Exit(1)
	}

	file, err := os.Open(*localFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	var cdc dedup.CDC
	if *useFastCDC {
		fmt.Println("Using FastCDC chunking algorithm.")
		cdc = &dedup.FastCDC{
			MinChunkSize: *minSize,
			AvgChunkSize: *avgSize,
			MaxChunkSize: *maxSize,
		}
	} else {
		fmt.Println("Using FixedCDC chunking algorithm.")
		cdc = &dedup.FixedCDC{ChunkSize: *avgSize}
	}

	chunker, err := cdc.NewChunker(file)
	if err != nil {
		fmt.Printf("Error creating chunker: %v\n", err)
		os.Exit(1)
	}

	var chunks []dedup.Chunk
	var totalSize int64
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error getting next chunk: %v\n", err)
			os.Exit(1)
		}
		chunks = append(chunks, chunk)
		totalSize += int64(chunk.Len)
	}

	fmt.Printf("Successfully chunked file into %d chunks, total size: %d bytes\n", len(chunks), totalSize)

	dedup.CalcFPs(chunks)
	for i, chunk := range chunks {
		fmt.Printf("Chunk %d: len=%d, fp=%s\n", i+1, chunk.Len, internal.StringToHex(chunk.FP))
	}
}
