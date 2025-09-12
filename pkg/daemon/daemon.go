package daemon

import (
	"os"

	"github.com/sevlyar/go-daemon"
)

// WasReborn checks if the current process is a daemonized child by checking
// for an environment variable set by the go-daemon library.
func WasReborn() bool {
	return daemon.WasReborn()
}

// UnsetMark unsets the environment variable used to mark the child process.
// This should be called by the child process after it has been identified.
func UnsetMark() {
	os.Unsetenv(daemon.MARK_NAME)
}

// Daemonize forks the current process into a background daemon. The parent process will exit,
// and the child process will continue execution. It returns a non-nil process if it's the
// parent, and nil if it's the child.
func Daemonize(pidFile, logFile string, args []string) (*os.Process, error) {
	// If logFile is empty, it will redirect to /dev/null
	if logFile == "" {
		logFile = os.DevNull
	}

	cntxt := &daemon.Context{
		PidFileName: pidFile,
		PidFilePerm: 0644,
		LogFileName: logFile,
		LogFilePerm: 0640,
		WorkDir:     "/",
		Umask:       027,
		Args:        args,
	}

	d, err := cntxt.Reborn()
	if err != nil {
		return nil, err
	}
	return d, nil
}
