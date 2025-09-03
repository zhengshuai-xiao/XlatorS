// Copyright 2015 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

var mu sync.Mutex
var loggers = make(map[string]*logHandle)

var syslogHook logrus.Hook
var framePlaceHolder = runtime.Frame{Function: "???", File: "???", Line: 0}

type logHandle struct {
	logrus.Logger

	name     string
	logid    string
	pid      int
	lvl      *logrus.Level
	colorful bool
}

func (l *logHandle) Format(e *logrus.Entry) ([]byte, error) {
	lvl := e.Level
	if l.lvl != nil {
		lvl = *l.lvl
	}
	lvlStr := strings.ToUpper(lvl.String())
	if l.colorful {
		var color int
		switch lvl {
		case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
			color = 31 // RED
		case logrus.WarnLevel:
			color = 33 // YELLOW
		case logrus.InfoLevel:
			color = 34 // BLUE
		default: // logrus.TraceLevel, logrus.DebugLevel
			color = 35 // MAGENTA
		}
		lvlStr = fmt.Sprintf("\033[1;%dm%s\033[0m", color, lvlStr)
	}
	const timeFormat = "2006/01/02 15:04:05.000000"
	caller := e.Caller
	if caller == nil { // for unknown reason, sometimes e.Caller is nil
		caller = &framePlaceHolder
	}
	str := fmt.Sprintf("%s%v %s[%d] <%v>: %v [%s@%s:%d]",
		l.logid,
		e.Time.Format(timeFormat),
		l.name,
		l.pid,
		lvlStr,
		strings.TrimRight(e.Message, "\n"),
		MethodName(caller.Function),
		path.Base(caller.File),
		caller.Line)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	return []byte(str), nil
}

// Returns a human-readable method name, removing internal markers added by Go
func MethodName(fullFuncName string) string {
	firstSlash := strings.Index(fullFuncName, "/")
	if firstSlash != -1 && firstSlash < len(fullFuncName)-1 {
		fullFuncName = fullFuncName[firstSlash+1:]
	}
	lastDot := strings.LastIndex(fullFuncName, ".")
	if lastDot == -1 || lastDot == len(fullFuncName)-1 {
		return fullFuncName
	}
	method := fullFuncName[lastDot+1:]
	// avoid func1
	if strings.HasPrefix(method, "func") && method[4] >= '0' && method[4] <= '9' {
		candidate := MethodName(fullFuncName[:lastDot])
		if candidate != "" {
			method = candidate
		}
	}
	// avoid init.3
	if len(method) == 1 && method[0] >= '0' && method[0] <= '9' {
		candidate := MethodName(fullFuncName[:lastDot])
		if candidate != "" {
			method = candidate
		}
	}
	return method
}

// for aws.Logger
func (l *logHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

func newLogger(name string) *logHandle {
	l := &logHandle{Logger: *logrus.New(), name: name, pid: os.Getpid()}
	l.Formatter = l
	if syslogHook != nil {
		l.AddHook(syslogHook)
	}
	l.SetReportCaller(true)
	return l
}

// GetLogger returns a logger mapped to `name`
func GetLogger(name string) *logHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		return logger
	}
	logger := newLogger(name)
	loggers[name] = logger
	return logger
}

// SetLogLevel sets Level to all the loggers in the map
func SetLogLevel(lvl logrus.Level) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.Level = lvl
	}
}

func DisableLogColor() {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.colorful = false
	}
}

func SetOutFile(name string) {
	// 使用 file-rotatelogs 实现日志轮转
	// 这会创建类似 "name.20060102" 的日志文件，并自动创建一个指向最新日志的软链接 "name"
	logf, err := rotatelogs.New(
		name+".%Y%m%d",
		rotatelogs.WithLinkName(name),              // 创建一个软链接到最新的日志文件
		rotatelogs.WithMaxAge(7*24*time.Hour),      // 最多保留7天的日志
		rotatelogs.WithRotationTime(24*time.Hour),  // 每天轮转一次
		rotatelogs.WithRotationSize(100*1024*1024), // 每个文件最大100MB
	)

	if err != nil {
		logrus.Fatalf("Failed to open log file %s: %v", name, err)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.SetOutput(logf)
		logger.colorful = false
	}
}

func SetOutput(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.SetOutput(w)
	}
}

func SetLogID(id string) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.logid = id
	}
}

func GetDefaultLogDir() string {
	var defaultLogDir = "/var/log"
	switch runtime.GOOS {
	case "linux":
		if os.Getuid() == 0 {
			break
		}
		fallthrough
	case "darwin":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Warn(err)
			homeDir = defaultLogDir
		}
		defaultLogDir = path.Join(homeDir, ".xlators")
	case "windows":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
		}
		defaultLogDir = path.Join(homeDir, ".xlators")
	}
	return defaultLogDir
}

func GetCurrentFuncName() string {
	// runtime.Caller(1) 表示获取调用当前函数的函数信息
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		return "unknown"
	}
	// 获取函数信息并返回函数名
	return runtime.FuncForPC(pc).Name()
}
