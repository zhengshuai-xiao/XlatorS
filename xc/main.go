package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "xc",
		Usage: "A collection of command-line utilities for XlatorS",
		Commands: []*cli.Command{
			uploadCmd(),
			calcFPCmd(),
			getAWSCmd(),
			gcTriggerCmd(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
