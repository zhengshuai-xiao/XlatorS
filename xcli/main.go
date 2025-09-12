package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "xcli",
		Usage: "A simple S3 client and admin tool for XlatorS",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoint",
				Value:   "localhost:9000",
				Usage:   "S3 gateway endpoint",
				EnvVars: []string{"XCLI_ENDPOINT"},
			},
			&cli.StringFlag{
				Name:    "access-key",
				Value:   "minio",
				Usage:   "S3 access key",
				EnvVars: []string{"MINIO_ROOT_USER"},
			},
			&cli.StringFlag{
				Name:    "secret-key",
				Value:   "minioadmin",
				Usage:   "S3 secret key",
				EnvVars: []string{"MINIO_ROOT_PASSWORD"},
			},
			&cli.BoolFlag{
				Name:  "no-ssl",
				Usage: "Disable SSL for S3 connection",
				Value: true,
			},
		},
		Commands: []*cli.Command{
			makeBucketCmd(),
			removeBucketCmd(),
			uploadCmd(),
			downloadCmd(),
			deleteCmd(),
			gcTriggerCmd(),
			getAWSCmd(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
