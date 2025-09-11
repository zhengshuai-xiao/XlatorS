#!/bin/bash

# #############################################################################
#
# MinIO Management Script
#
# This script starts, stops, or restarts the MinIO server.
#
# #############################################################################

# --- MinIO Configuration ---
MINIO_ROOT_USER=${MINIO_ROOT_USER:-"minio"}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-"minioadmin"}
MINIO_DATA_DIR=${MINIO_DATA_DIR:-"/data"}
MINIO_BINARY=${MINIO_BINARY:-"/workspace/minio/minio"}
MINIO_ADDRESS=${MINIO_ADDRESS:-"127.0.0.1:9001"}

# --- Functions ---

start_minio() {
    echo "Starting MinIO..."
    if pgrep -f "minio server" > /dev/null; then
        echo "MinIO is already running."
        return 0
    fi

    # Set credentials and data directory using environment variables
    export MINIO_ROOT_USER=${MINIO_ROOT_USER}
    export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}

    # Create MinIO data directory if it doesn't exist
    if [ ! -d "$MINIO_DATA_DIR" ]; then
        echo "MinIO data directory not found. Creating $MINIO_DATA_DIR"
        mkdir -p "$MINIO_DATA_DIR"
    fi

    # Start MinIO as a background process
    nohup ${MINIO_BINARY} server --address ${MINIO_ADDRESS} "$MINIO_DATA_DIR" > /dev/null 2>&1 &

    # Wait for MinIO to be ready by polling its health check endpoint
    echo "Waiting for MinIO to start..."
    local retries=10
    local wait_seconds=2
    for ((i=0; i<retries; i++)); do
        # Use curl to check the health endpoint. The /minio/health/live is standard.
        if curl -s -o /dev/null "http://${MINIO_ADDRESS}/minio/health/live"; then
            echo "MinIO started successfully."
            return 0
        fi
        sleep ${wait_seconds}
    done

    echo "Failed to start MinIO after $((retries * wait_seconds)) seconds."
    # Try to kill the process if it started but is not healthy
    if pgrep -f "minio server" > /dev/null; then
        pkill -f "minio server"
    fi
    return 1
}

stop_minio() {
    echo "Stopping MinIO..."
    if pgrep -f "minio server" > /dev/null; then
        pkill -f "minio server"
        echo "MinIO stopped."
    else
        echo "MinIO is not running."
    fi
}

# --- Main Program ---
# This block will only run if the script is executed directly, not if it's sourced.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    case "$1" in
        start)
            start_minio
            ;;
        stop)
            stop_minio
            ;;
        restart)
            stop_minio
            sleep 2
            start_minio
            ;;
        *)
            echo "Usage: $0 {start|stop|restart}"
            exit 1
            ;;
    esac
fi
