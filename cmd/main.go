package cmd

import (
	"strings"
	"syscall"

	"github.com/urfave/cli/v2"
	"github.com/zhengshuai-xiao/XlatorS/internal"
)

var logger = internal.GetLogger("XlatorS_cmd")

func Main(args []string) error {
	// we have to call this because gspt removes all arguments
	//gspt.SetProcTitle(strings.Join(os.Args, " "))
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print version only",
	}
	app := &cli.App{
		Name:                 "xlators",
		Usage:                "A POSIX file system built on Redis and object storage.",
		Version:              internal.Version(),
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                globalFlags(),
		Commands: []*cli.Command{
			cmdGateway(),
		},
	}

	err := app.Run(reorderOptions(app, args))
	if errno, ok := err.(syscall.Errno); ok && errno == 0 {
		err = nil
	}

	return err
}

func reorderOptions(app *cli.App, args []string) []string {
	var newArgs = []string{args[0]}
	var others []string
	globalFlags := append(app.Flags, cli.VersionFlag)
	for i := 1; i < len(args); i++ {
		option := args[i]
		if ok, hasValue := isFlag(globalFlags, option); ok {
			newArgs = append(newArgs, option)
			if hasValue {
				i++
				if i >= len(args) {
					logger.Fatalf("option %s requires value", option)
				}
				newArgs = append(newArgs, args[i])
			}
		} else {
			others = append(others, option)
		}
	}
	// no command
	if len(others) == 0 {
		return newArgs
	}
	cmdName := others[0]
	var cmd *cli.Command
	for _, c := range app.Commands {
		if c.Name == cmdName {
			cmd = c
			break
		}
	}
	if cmd == nil {
		// can't recognize the command, skip it
		return append(newArgs, others...)
	}

	newArgs = append(newArgs, cmdName)
	args, others = others[1:], nil
	// -h is valid for all the commands
	cmdFlags := append(cmd.Flags, cli.HelpFlag)
	for i := 0; i < len(args); i++ {
		option := args[i]
		if ok, hasValue := isFlag(cmdFlags, option); ok {
			newArgs = append(newArgs, option)
			if hasValue && len(args[i+1:]) > 0 {
				i++
				newArgs = append(newArgs, args[i])
			}
		} else {
			if strings.HasPrefix(option, "-") && !internal.StringContains(args, "--generate-bash-completion") {
				logger.Fatalf("unknown option: %s", option)
			}
			others = append(others, option)
		}
	}
	return append(newArgs, others...)
}

func isFlag(flags []cli.Flag, option string) (bool, bool) {
	if !strings.HasPrefix(option, "-") {
		return false, false
	}
	// --V or -v work the same
	option = strings.TrimLeft(option, "-")
	for _, flag := range flags {
		_, isBool := flag.(*cli.BoolFlag)
		for _, name := range flag.Names() {
			if option == name || strings.HasPrefix(option, name+"=") {
				return true, !isBool && !strings.Contains(option, "=")
			}
		}
	}
	return false, false
}
