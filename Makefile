export GO111MODULE=on

xlators := xlators
upload_file := upload_file
calc_fp:=calc_fp
getObject_aws:=getObject_aws
gctrigger := gc_trigger

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
	@echo "building release"
	go build -ldflags="$(LDFLAGS)" -o $(xlators) main.go
	go build -ldflags="$(LDFLAGS)" -o $(upload_file) utils/upload_file.go
	go build -ldflags="$(LDFLAGS)" -o $(calc_fp) utils/calcFP.go
	go build -ldflags="$(LDFLAGS)" -o $(getObject_aws) utils/getObject4AWS.go
	go build -ldflags="$(LDFLAGS)" -o $(gctrigger) utils/gc_trigger.go
dbuild:
	go version
	@echo "building debug"
	go build -gcflags "all=-N -l"  -o $(xlators) main.go
	go build -gcflags "all=-N -l"  -o $(upload_file) utils/upload_file.go
	go build -gcflags "all=-N -l"  -o $(calc_fp) utils/calcFP.go
	go build -gcflags "all=-N -l"  -o $(gctrigger) utils/gc_trigger.go
#go build -ldflags="$(LDFLAGS)" -o $(xlators) main.go
#-gcflags "all=-N -l"

run:
	go run main.go

clean:
	rm -f $(xlators) $(upload_file) $(calc_fp) $(gctrigger)

deps:
	go mod tidy
