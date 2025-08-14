package cmd

import (
	"path"

	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/S3Store/internal"
)

func cmdGateway() *cli.Command {
	selfFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "log",
			Usage: "path for gateway log",
			Value: path.Join(internal.GetDefaultLogDir(), "s3store-gateway.log"),
		},
		&cli.StringFlag{
			Name:  "access-log",
			Usage: "path for s3store access log",
		},
		&cli.BoolFlag{
			Name:    "background",
			Aliases: []string{"d"},
			Usage:   "run in background",
		},
		&cli.BoolFlag{
			Name:  "no-banner",
			Usage: "disable MinIO startup information",
		},
		&cli.BoolFlag{
			Name:  "multi-buckets",
			Usage: "use top level of directories as buckets",
		},
		&cli.BoolFlag{
			Name:  "keep-etag",
			Usage: "keep the ETag for uploaded objects",
		},
		&cli.StringFlag{
			Name:  "umask",
			Value: "022",
			Usage: "umask for new files and directories in octal",
		},
		&cli.BoolFlag{
			Name:  "object-tag",
			Usage: "enable object tagging api",
		},
		&cli.BoolFlag{
			Name:  "object-meta",
			Usage: "enable object metadata api",
		},
		&cli.BoolFlag{
			Name:  "head-dir",
			Usage: "allow HEAD request on directories",
		},
		&cli.BoolFlag{
			Name:  "hide-dir-object",
			Usage: "hide the directories created by PUT Object API",
		},
		&cli.StringFlag{
			Name:  "domain",
			Usage: "domain for virtual-host-style requests",
		},
		&cli.StringFlag{
			Name:  "refresh-iam-interval",
			Value: "5m",
			Usage: "interval to reload gateway IAM from configuration",
		},
		&cli.StringFlag{
			Name:  "mountpoint",
			Value: "s3gateway",
			Usage: "the mount point for current volume (to follow symlink)",
		},
	}

	return &cli.Command{
		Name:      "gateway",
		Action:    gateway,
		Category:  "SERVICE",
		Usage:     "Start an S3-compatible gateway",
		ArgsUsage: "META-URL ADDRESS",
		Description: `
It is implemented based on the MinIO S3 Gateway. Before starting the gateway, you need to set
MINIO_ROOT_USER and MINIO_ROOT_PASSWORD environment variables, which are the access key and secret
key used for accessing S3 APIs.

Examples:
$ export MINIO_ROOT_USER=admin
$ export MINIO_ROOT_PASSWORD=12345678
$ s3store gateway redis://localhost localhost:9000`,
		Flags: expandFlags(selfFlags, clientFlags(0), shareInfoFlags()),
	}
}

func gateway(c *cli.Context) error {
	//return startGateway(c)
	return nil
}
