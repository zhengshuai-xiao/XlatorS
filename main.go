package main

//go:generate go install tool

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/zhengshuai-xiao/S3Store/cmd"
	"github.com/zhengshuai-xiao/S3Store/internal"
)

var logger = internal.GetLogger("s3store_main")

func main() {
	internal.SetLogLevel(logrus.TraceLevel)
	logger.Trace("Trace log is enabled")
	err := cmd.Main(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}
