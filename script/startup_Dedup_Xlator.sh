#!/bin/bash

# Get the absolute path of the directory where the script is located.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

# Source the management scripts using an absolute path to make the script runnable from any directory.
source "${SCRIPT_DIR}/manage_redis.sh"
source "${SCRIPT_DIR}/manage_minio.sh"

# #############################################################################
#
# XlatorS Dedup Gateway Startup Script
#
# This script starts the XlatorS gateway with the Dedup xlator.
# It allows choosing between 'posix' and 's3' data storage backends.
#
# #############################################################################

# --- Script Configuration ---
set -e # Exit immediately if a command exits with a non-zero status.

# Function to print usage instructions
usage() {
    echo "Usage: $0 [posix|s3]"
    echo "  posix: Start Dedup xlator with local POSIX filesystem as the data backend."
    echo "  s3:    Start Dedup xlator with an S3-compatible service as the data backend."
    echo ""
    echo "Before running, ensure dependent services (Redis, MinIO for S3 backend) are running."
    exit 1
}

# --- Environment and Parameter Setup ---

# 1. Check for required argument
if [ "$#" -ne 1 ]; then
    echo "Error: Missing required argument."
    usage
fi
BACKEND_TYPE=$1

# 2. Set credentials for the XlatorS S3 gateway API
# These are the access key and secret key you will use with an S3 client.
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minioadmin

# 3. (Optional) Configure FastCDC chunking parameters
export XL_DEDUP_FASTCDC_MIN_SIZE=65536   # 64KiB
export XL_DEDUP_FASTCDC_AVG_SIZE=131072  # 128KiB
export XL_DEDUP_FASTCDC_MAX_SIZE=262144  # 256KiB

# 4. Define service parameters
BINARY="${SCRIPT_DIR}/../bin/xlators"
XLATOR_TYPE="Dedup"
LISTEN_ADDR="127.0.0.1:9000"
META_ADDR="127.0.0.1:6379/1" # Redis address
LOG_LEVEL="trace"
LOG_DIR="/var/log/xlator/"
DOWNLOAD_CACHE="/dedup_data" # Local cache for data objects
S3_BACKEND_ADDR="http://127.0.0.1:9001"   # Address of the backend S3 storage

# --- Pre-run Checks and Setup -----
: '
# Check and kill existing process on the same port
echo "Checking and killing existing process on the same port..."
PORT=${LISTEN_ADDR##*:}
if [[ ! -z "$PORT" ]]; then
    # Find PID listening on the port. -t gives terse output (only PID)
    PID=$(lsof -i :$PORT -t 2>/dev/null)

    if [[ ! -z "$PID" ]]; then
        # Check if the process name is 'xlators'
        PNAME=$(ps -p $PID -o comm= | tr -d '[:space:]')
        if [[ "$PNAME" == "xlators" ]]; then
            echo "Found existing 'xlators' process (PID: $PID) on port $PORT. Terminating it..."
            kill $PID
            # Wait a moment for graceful shutdown
            sleep 5
            # Check if its still alive, if so, force kill
            if ps -p $PID > /dev/null; then
                echo "Process $PID did not terminate gracefully. Forcing kill..."
                kill -9 $PID
                sleep 1
            fi
            echo "Existing process terminated."
        fi
    fi
fi
'
# Check if the binary exists
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary '$BINARY' not found. Please run 'make build' first."
    exit 1
fi

# Create necessary directories
mkdir -p "$LOG_DIR"
mkdir -p "$DOWNLOAD_CACHE"

# --- Build and Execute Command ---

BASE_CMD="$BINARY gateway \
    --address $LISTEN_ADDR \
    --xlator $XLATOR_TYPE \
    --meta-addr $META_ADDR \
    --loglevel $LOG_LEVEL \
    --downloadCache $DOWNLOAD_CACHE"
#    --logdir $LOG_DIR \
echo "Starting XlatorS Dedup Gateway..."

start_redis

if [ "$BACKEND_TYPE" == "posix" ]; then
    FINAL_CMD="$BASE_CMD --ds-backend posix"
elif [ "$BACKEND_TYPE" == "s3" ]; then
    start_minio
    FINAL_CMD="$BASE_CMD --ds-backend s3 --backend-addr $S3_BACKEND_ADDR"
else
    echo "Error: Invalid backend type '$BACKEND_TYPE'."
    usage
fi

echo "---------------------------------"
echo "Executing command:"
echo "$FINAL_CMD"
echo "---------------------------------"

# Execute the final command

eval $FINAL_CMD
