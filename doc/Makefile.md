# Makefile Guide

This document explains the `Makefile` used in the XlatorS project.

## Overview

The `Makefile` provides a set of targets to automate common development tasks such as building, testing, and cleaning the project.

## Targets

Here is a list of available targets and their descriptions. You can see this list by running `make help`.

-   `all`: This is the default target. It runs the `build` target.
-   `build`: Compiles all source code and creates release-ready binaries in the `./bin` directory. This includes the main `xlators` application and all utilities.
-   `dbuild`: Compiles all source code with debugging information enabled. This is useful for development and debugging with tools like Delve.
-   `rebuild`: Cleans the project and then builds all binaries from scratch.
-   `test`: Runs all unit tests in the project.
-   `deps`: Runs `go mod tidy` to ensure the `go.mod` file is consistent with the source code.
-   `clean`: Removes all compiled binaries from the `./bin` directory.
-   `help`: Displays a help message with all available targets and their descriptions.

## Usage Examples

### Build for Release

To create optimized binaries for production, run:

```bash
make build
```
Or simply:
```bash
make
```

### Build for Debugging

To build binaries with debugging symbols:

```bash
make dbuild
```

### Run Tests

To execute all unit tests:

```bash
make test
```

### Clean Project

To remove all generated binaries:

```bash
make clean
```