package internal

import (
	"errors"
	"syscall"
)

var (
	ENOTSUP        = errors.New("not supported")
	ErrFuncTimeout = errors.New("function timeout")
	ErrSkipped     = errors.New("skipped")
	ErrExtlink     = syscall.Errno(1000)
)
