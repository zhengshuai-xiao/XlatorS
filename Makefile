# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GORUN=$(GOMOD) run

# Project variables
BINARY_NAME=xlators
BINARY_DIR=./bin
LDFLAGS = -s -w

# Source files
MAIN_SRC = main.go
XC_SRC_DIR = xcli

# Target binaries
MAIN_BINARY = $(BINARY_DIR)/$(BINARY_NAME)
XC_BINARY = $(BINARY_DIR)/xc

ALL_BINARIES = $(MAIN_BINARY) $(XC_BINARY)

# Docker parameters
DOCKER_CMD=docker
DOCKER_IMAGE_NAME=xlators-app
DOCKER_TAG=latest

# Default target
all: build

# Phony targets are not files
.PHONY: all build dbuild rebuild test run clean cleancache deps help

# Build targets
build: $(ALL_BINARIES) ## Build all binaries for release

# Build the Docker image
docker-build: build
	@echo "Building Docker image $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)..."
	$(DOCKER_CMD) build -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .
	
dbuild: ## Build all binaries for debug
	@echo "--> Building debug binaries..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -gcflags "all=-N -l" -o $(MAIN_BINARY) $(MAIN_SRC)
	$(GOBUILD) -gcflags "all=-N -l" -o $(XC_BINARY) ./$(XC_SRC_DIR)

$(MAIN_BINARY): $(MAIN_SRC)
	@echo "--> Building main binary: $(BINARY_NAME)"
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -ldflags="$(LDFLAGS)" -o $@ $<

$(XC_BINARY): $(wildcard $(XC_SRC_DIR)/*.go)
	@echo "--> Building utility: xc"
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -ldflags="$(LDFLAGS)" -o $@ ./$(XC_SRC_DIR)

rebuild: clean build ## Rebuild all binaries from scratch

# Other targets
test: ## Run unit tests
	@echo "--> Running unit tests..."
	$(GOTEST) -v ./...

deps: ## Tidy go modules
	@echo "--> Tidying go modules..."
	$(GOMOD) tidy

clean: ## Clean up built binaries and directory
	@echo "--> Cleaning up..."
	$(GOCLEAN)
	rm -rf $(BINARY_DIR)

help: ## Show this help message
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build docker-build dbuild rebuild test deps clean help 