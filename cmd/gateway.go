package cmd

import (
	"os"
	"path"
	"strconv"

	mcli "github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/xlator/dedup"
	s3forward "github.com/zhengshuai-xiao/XlatorS/xlator/s3forward"
)

func cmdGateway() *cli.Command {
	selfFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "log",
			Usage: "path for gateway log",
			Value: path.Join(internal.GetDefaultLogDir(), "xlators-gateway.log"),
		},
		&cli.StringFlag{
			Name:  "loglevel",
			Usage: "log level for gateway: trace/info/warn/error",
			Value: "info",
		},
		&cli.StringFlag{
			Name:  "access-log",
			Usage: "path for xlators access log",
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
		&cli.StringFlag{
			Name:  "backend-addr",
			Value: "http://127.0.0.1:1234",
			Usage: "the address of the backend storage",
		},
		&cli.StringFlag{
			Name:  "address",
			Value: "127.0.0.1:9000",
			Usage: "the S3 API listen address",
		},
		&cli.StringFlag{
			Name:  "meta-addr",
			Value: "127.0.0.1:6379/1",
			Usage: "the address of the metadata storage",
		},
		&cli.StringFlag{
			Name:  "xlator",
			Value: "Dedup",
			Usage: "the name of the translator: S3Forward/Dedup",
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
			$ xlators gateway redis://localhost localhost:9000`,
		Flags: expandFlags(selfFlags, clientFlags(0), shareInfoFlags()),
	}

}

var xobject minio.ObjectLayer

func gateway(c *cli.Context) error {
	//setup(c, 2)
	ak := os.Getenv("MINIO_ROOT_USER")
	if ak == "" {
		ak = os.Getenv("MINIO_ACCESS_KEY")
	}
	if len(ak) < 3 {
		logger.Fatalf("MINIO_ROOT_USER should be specified as an environment variable with at least 3 characters")
	}
	sk := os.Getenv("MINIO_ROOT_PASSWORD")
	if sk == "" {
		sk = os.Getenv("MINIO_SECRET_KEY")
	}
	if len(sk) < 8 {
		logger.Fatalf("MINIO_ROOT_PASSWORD should be specified as an environment variable with at least 8 characters")
	}
	if c.IsSet("domain") {
		os.Setenv("MINIO_DOMAIN", c.String("domain"))
	}

	if c.IsSet("refresh-iam-interval") {
		os.Setenv("MINIO_REFRESH_IAM_INTERVAL", c.String("refresh-iam-interval"))
	}

	switch c.String("loglevel") {
	case "trace":
		internal.SetLogLevel(logrus.TraceLevel)
	case "debug":
		internal.SetLogLevel(logrus.DebugLevel)
	case "info":
		internal.SetLogLevel(logrus.InfoLevel)
	case "warn":
		internal.SetLogLevel(logrus.WarnLevel)
	case "error":
		internal.SetLogLevel(logrus.ErrorLevel)
	default:
		internal.SetLogLevel(logrus.InfoLevel)
	}
	//metaAddr := c.Args().Get(0)
	listenAddr := c.String("address")
	//conf, jfs := initForSvc(c, c.String("mountpoint"), "s3gateway", metaAddr, listenAddr)

	umask, err := strconv.ParseUint(c.String("umask"), 8, 16)
	if err != nil {
		logger.Fatalf("invalid umask %s: %s", c.String("umask"), err)
	}

	readonly := c.Bool("read-only")
	if c.String("xlator") == s3forward.XlatorName {
		xobject, err = s3forward.NewS3Object(
			&internal.Config{
				Xlator:      c.String("xlator"),
				MultiBucket: c.Bool("multi-buckets"),
				KeepEtag:    c.Bool("keep-etag"),
				Umask:       uint16(umask),
				ObjTag:      c.Bool("object-tag"),
				ObjMeta:     c.Bool("object-meta"),
				HeadDir:     c.Bool("head-dir"),
				HideDir:     c.Bool("hide-dir-object"),
				ReadOnly:    readonly,
				BackendAddr: c.String("backend-addr"),
				MetaDriver:  "redis",
				MetaAddr:    c.String("meta-addr"),
			},
		)
	} else if c.String("xlator") == dedup.XlatorName {
		xobject, err = dedup.NewXlatorDedup(
			&internal.Config{
				Xlator:      c.String("xlator"),
				MultiBucket: c.Bool("multi-buckets"),
				KeepEtag:    c.Bool("keep-etag"),
				Umask:       uint16(umask),
				ObjTag:      c.Bool("object-tag"),
				ObjMeta:     c.Bool("object-meta"),
				HeadDir:     c.Bool("head-dir"),
				HideDir:     c.Bool("hide-dir-object"),
				ReadOnly:    readonly,
				BackendAddr: c.String("backend-addr"),
				MetaDriver:  "redis",
				MetaAddr:    c.String("meta-addr"),
			},
		)
	}

	if err != nil {
		return err
	}
	if readonly {
		os.Setenv("META_READ_ONLY", "1")
	} else {
		/*if _, err := s3sGateway.GetBucketInfo(context.Background(), minio.MinioMetaBucket); errors.As(err, &minio.BucketNotFound{}) {
			if err := s3sGateway.MakeBucketWithLocation(context.Background(), minio.MinioMetaBucket, minio.BucketOptions{}); err != nil {
				logger.Fatalf("init MinioMetaBucket error %s: %s", minio.MinioMetaBucket, err)
			}
		}*/
		logger.Info("not implement")
	}

	args := []string{"server", "--address", listenAddr, "--anonymous", "/data"}
	if c.Bool("no-banner") {
		args = append(args, "--quiet")
	}
	app := &mcli.App{
		Action: gateway2,
		Flags: []mcli.Flag{
			mcli.StringFlag{
				Name:  "address",
				Value: ":9000",
				Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
			},
			mcli.BoolFlag{
				Name:  "anonymous",
				Usage: "hide sensitive information from logging",
			},
			mcli.BoolFlag{
				Name:  "json",
				Usage: "output server logs and startup information in json format",
			},
			mcli.BoolFlag{
				Name:  "quiet",
				Usage: "disable MinIO startup information",
			},
		},
	}
	return app.Run(args)
}

func gateway2(ctx *mcli.Context) error {
	logger.Info("start gateway2")
	minio.ServerMain4XlatorS(ctx, xobject)
	logger.Info("end gateway2")
	return nil
}
