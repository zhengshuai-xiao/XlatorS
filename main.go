package main

//go:generate go install tool

import (
	"os"

	"github.com/zhengshuai-xiao/XlatorS/cmd"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

var logger = internal.GetLogger("XlatorS_main")

func main() {
	//internal.SetLogLevel(logrus.TraceLevel)
	//internal.SetLogLevel(logrus.InfoLevel)
	logger.Trace("Trace log is enabled")
	err := cmd.Main(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}
