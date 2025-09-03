# ---- Build Stage ----
# Use a specific Go version on Alpine for a smaller build environment.
FROM golang:1.24.6-alpine AS builder

# Install build dependencies required by the Makefile.
RUN apk add --no-cache make git

# Set the working directory inside the container.
WORKDIR /app

# Copy go module files and download dependencies first to leverage Docker's layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code.
COPY . .

# Build the application and utilities using the Makefile.
# This ensures a consistent build process.
RUN make build

# ---- Runtime Stage ----
# Use a minimal, secure base image for the final container.
FROM alpine:latest

# Install ca-certificates for HTTPS and TLS communication.
# TODO: not support TLS
#RUN apk add --no-cache ca-certificates

# Copy the compiled binaries from the builder stage.
COPY --from=builder /app/bin/xlators /usr/local/bin/xlators
COPY --from=builder /app/bin/xc /usr/local/bin/xc

# Copy license files for compliance.
COPY LICENSE NOTICE /

# Expose the default S3 API port.
EXPOSE 9000

#ENV MINIO_ROOT_USER=minio \
#    MINIO_ROOT_PASSWORD=minioadmin

# The entrypoint is the main application binary.
# Arguments should be provided via the `docker run` command.
ENTRYPOINT ["/usr/local/bin/xlators"]
VOLUME ["/xlators_data", "/xlators_log"]
# Default command to run the gateway with your specified configuration.
CMD [""]