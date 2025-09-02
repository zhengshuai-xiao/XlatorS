package main

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/xlator/dedup"
)

func calcFPCmd() *cli.Command {
	return &cli.Command{
		Name:  "calcfp",
		Usage: "Calculate fingerprints for a local file using different chunking algorithms",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "file", Required: true, Usage: "Path to local file"},
			&cli.IntFlag{Name: "avg", Value: 128 * 1024, Usage: "Average chunk size for CDC"},
			&cli.IntFlag{Name: "min", Value: 64 * 1024, Usage: "Min chunk size for FastCDC"},
			&cli.IntFlag{Name: "max", Value: 256 * 1024, Usage: "Max chunk size for FastCDC"},
			&cli.BoolFlag{Name: "fastcdc", Usage: "Use FastCDC instead of FixedCDC"},
		},
		Action: func(c *cli.Context) error {
			file, err := os.Open(c.String("file"))
			if err != nil {
				return fmt.Errorf("error opening file: %w", err)
			}
			defer file.Close()

			var cdc dedup.CDC
			if c.Bool("fastcdc") {
				fmt.Println("Using FastCDC chunking algorithm.")
				cdc = &dedup.FastCDC{
					MinChunkSize: c.Int("min"),
					AvgChunkSize: c.Int("avg"),
					MaxChunkSize: c.Int("max"),
				}
			} else {
				fmt.Println("Using FixedCDC chunking algorithm.")
				cdc = &dedup.FixedCDC{ChunkSize: c.Int("avg")}
			}

			chunker, err := cdc.NewChunker(file)
			if err != nil {
				return fmt.Errorf("error creating chunker: %w", err)
			}

			var chunks []dedup.Chunk
			var totalSize int64
			for {
				chunk, err := chunker.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("error getting next chunk: %w", err)
				}
				chunks = append(chunks, chunk)
				totalSize += int64(chunk.Len)
			}

			fmt.Printf("Successfully chunked file into %d chunks, total size: %d bytes\n", len(chunks), totalSize)

			dedup.CalcFPs(chunks)
			for i, chunk := range chunks {
				fmt.Printf("Chunk %d: len=%d, fp=%s\n", i+1, chunk.Len, internal.StringToHex(chunk.FP))
			}
			return nil
		},
	}
}
