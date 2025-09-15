package cmd

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"syscall"

	mcli "github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	"github.com/zhengshuai-xiao/XlatorS/pkg/daemon"
	"github.com/zhengshuai-xiao/XlatorS/xlator/dedup"
	s3forward "github.com/zhengshuai-xiao/XlatorS/xlator/s3forward"
)

func cmdGateway() *cli.Command {
	selfFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "logdir",
			Usage: "path for gateway log",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "loglevel",
			Usage: "log level for gateway: trace/info/warn/error",
			Value: "info",
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
			Name:  "compression",
			Usage: "compress data with the specified algorithm: none/snappy/zlib",
			Value: "snappy",
		},
		&cli.StringFlag{
			Name:  "downloadCache",
			Value: "/dedup_data/",
			Usage: "the download Cache path for Dedup",
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
			Name:  "ds-backend",
			Value: string(dedup.DObjBackendPOSIX),
			Usage: fmt.Sprintf("Deduplication data store backend type ('%s' or '%s')", dedup.DObjBackendPOSIX, dedup.DObjBackendS3),
		},
		&cli.StringFlag{
			Name:  "xlator",
			Value: "Dedup",
			Usage: "the name of the translator: S3Forward/Dedup/CryptoCompress",
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
	// Handle daemonization first. If this function returns true, it means
	// the current process is the parent and should exit gracefully.
	if shouldExit, err := handleBackgroundMode(c); err != nil {
		// Use Fatalf to exit on configuration or daemonization errors.
		logger.Fatalf("Failed to start in background: %v", err)
	} else if shouldExit {
		return nil
	}

	var logFile string
	logDir := c.String("logdir")
	if logDir != "" {
		if c.String("xlator") != "" {
			xlatorLogFile := "xlator" + "-" + c.String("xlator") + ".log"
			logFile = path.Join(logDir, xlatorLogFile)
		}

		internal.SetOutFile(logFile)
	}

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
				Compression: c.String("compression"),
			},
		)
	} else if c.String("xlator") == dedup.XlatorName {
		xobject, err = dedup.NewXlatorDedup(
			&internal.Config{
				Xlator:        c.String("xlator"),
				MultiBucket:   c.Bool("multi-buckets"),
				KeepEtag:      c.Bool("keep-etag"),
				Umask:         uint16(umask),
				ObjTag:        c.Bool("object-tag"),
				ObjMeta:       c.Bool("object-meta"),
				HeadDir:       c.Bool("head-dir"),
				HideDir:       c.Bool("hide-dir-object"),
				ReadOnly:      readonly,
				BackendAddr:   c.String("backend-addr"),
				MetaDriver:    "redis",
				MetaAddr:      c.String("meta-addr"),
				DownloadCache: c.String("downloadCache"),
				DSBackendType: c.String("ds-backend"),
				Compression:   c.String("compression"),
			},
		)
	} else if c.String("xlator") == "CryptoCompress" { //cryptocompress.XlatorName {
		/*xobject, err = cryptocompress.NewXlatorCryptoCompress(
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
		)*/
		return fmt.Errorf("not implement for CryptoCompress")
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

// handleBackgroundMode checks for the --background flag and daemonizes the process if set.
// It returns true if the current process is the parent and should exit.
func handleBackgroundMode(c *cli.Context) (shouldExit bool, err error) {
	// If we are the child daemon process (marked by the env var), just clean up and continue.
	if daemon.WasReborn() {
		daemon.UnsetMark()
		return false, nil
	}

	// If the background flag is not set, do nothing.
	if !c.Bool("background") {
		return false, nil
	}

	// --- This block is only executed by the initial parent process. ---

	logDir := c.String("logdir")
	if logDir == "" {
		return false, fmt.Errorf("logdir must be specified when running in background mode")
	}
	if err := os.MkdirAll(logDir, 0750); err != nil {
		return false, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	pidFile := filepath.Join(logDir, "xlators.pid")
	// Check for stale PID file before attempting to daemonize.
	if _, statErr := os.Stat(pidFile); statErr == nil {
		pid, readErr := daemon.ReadPidFile(pidFile)
		if readErr == nil {
			proc, findErr := os.FindProcess(pid)
			if findErr == nil {
				// Sending signal 0 to a process on POSIX systems checks if it exists.
				if err := proc.Signal(syscall.Signal(0)); err != nil {
					// Process does not exist, so the PID file is stale.
					logger.Warnf("Found stale PID file for dead process %d. Removing it.", pid)
					if err := os.Remove(pidFile); err != nil {
						return false, fmt.Errorf("failed to remove stale PID file %s: %w", pidFile, err)
					}
				} else {
					// Process exists, so we cannot start a new daemon.
					return false, fmt.Errorf("daemon already running with PID %d", pid)
				}
			}
		}
	}

	// The arguments for the child process should be the same as the current one,
	// but without the --background flag to avoid an infinite loop.
	var newArgs []string
	for _, arg := range os.Args {
		if arg != "--background" && arg != "-d" {
			newArgs = append(newArgs, arg)
		}
	}

	d, err := daemon.Daemonize(
		pidFile,
		filepath.Join(logDir, "xlator-"+c.String("xlator")+".log"),
		newArgs,
	)
	if err != nil {
		return false, fmt.Errorf("unable to run in background: %w", err)
	}

	// If d is not nil, we are in the parent process and should exit.
	return d != nil, nil
}
