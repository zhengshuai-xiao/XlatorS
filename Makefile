export GO111MODULE=on

s3store := s3store

#REVISION := $(shell git rev-parse --short HEAD 2>/dev/null)
#REVISIONDATE := $(shell git log -1 --pretty=format:'%cd' --date short 2>/dev/null)

LDFLAGS = -s -w

SHELL = /bin/sh

ifdef STATIC
	LDFLAGS += -linkmode external -extldflags '-static'
	CC = /usr/bin/musl-gcc
	export CC
endif

build:
	go version
	go build -ldflags="$(LDFLAGS)" -o $(s3store) main.go

run:
	go run main.go

clean:
	rm -f $(s3store)

deps:
	go mod tidy
