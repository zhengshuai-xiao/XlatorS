#!/bin/bash

# This script performs an end-to-end test of the XlatorS deduplication service.
# It starts the required services using docker-compose, generates a test file,
# uploads it, downloads it, verifies its integrity, deletes it, triggers garbage
# collection, and finally verifies that the data has been cleaned up.

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Path Setup ---
# Determine the absolute path of the directory where the script is located, making it runnable from any directory.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT=$(cd -- "${SCRIPT_DIR}/.." &> /dev/null && pwd)

# --- Configuration ---
#COMPOSE_FILE="docker-compose.yml"
TEST_DIR="/tmp/test_data" # Local directory for cache volume
LOG_DIR="/tmp/test_log"   # Local directory for logs and pid file
TEST_FILE_NAME="test_100M.data"
TEST_FILE_PATH="/tmp/${TEST_FILE_NAME}"
DOWNLOADED_FILE_PATH="${TEST_FILE_PATH}.downloaded"
BUCKET_NAME="test.bucket"
OBJECT_NAME=${TEST_FILE_NAME}
XCLI_CMD="${PROJECT_ROOT}/bin/xc" # Absolute path to the xcli binary
GATEWAY_ENDPOINT="localhost:9000"
REDIS_ADDR="localhost:6379/0"
LOG_FILE="${LOG_DIR}/xlator-Dedup.log"

# --- Helper Functions ---
info() {
    echo "INFO: $1"
}

error() {
    echo "ERROR: $1" >&2
    exit 1
}

cleanup() {
    sleep 5
    info "--- Cleaning up ---"
    # Find the process ID by its command line, which is more reliable than a PID file.
    PID=$(pgrep -f "xlators gateway")
    if [ -n "${PID}" ]; then
        info "Stopping xlators process (PID: ${PID})..."
        # Kill the process and ignore errors if it's already gone.
        kill ${PID} || true
    else
        info "xlators process not found or already stopped."
    fi

    rm -rf ${TEST_DIR}
    rm -rf ${LOG_DIR}
    rm -f ${TEST_FILE_PATH}
    rm -f ${DOWNLOADED_FILE_PATH}
    info "Cleanup complete."
}

# --- Main Script ---

# Ensure cleanup runs on script exit or interruption
trap cleanup EXIT

info "--- Starting Test Environment ---"
# Build project binaries
(cd "${PROJECT_ROOT}" && make build)

# Create test directory for cache mount
mkdir -p ${TEST_DIR} ${LOG_DIR}

# Start services in detached mode
# The -d flag runs the gateway in the background. The PID is stored in LOG_DIR.
${PROJECT_ROOT}/bin/xlators gateway --xlator Dedup --ds-backend posix --meta-addr "${REDIS_ADDR}" --downloadCache ${TEST_DIR}  --loglevel trace --logdir ${LOG_DIR} -d
info "Waiting for services to start..."
sleep 10 # Give services time to initialize

info "--- Generating Test Data ---"
# Generate a 100MB file with 50% repetitive data to test deduplication
dd if=/dev/zero bs=1M count=50 | tr '\0' 'A' > ${TEST_FILE_PATH}
dd if=/dev/urandom bs=1M count=50 >> ${TEST_FILE_PATH}
ORIGINAL_MD5=$(md5sum ${TEST_FILE_PATH} | awk '{print $1}')
info "Generated test file ${TEST_FILE_PATH} with MD5: ${ORIGINAL_MD5}"

info "--- Creating Bucket ---"
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" mb ${BUCKET_NAME}
info "Bucket ${BUCKET_NAME} created or already exists."

info "--- Running Upload Test ---"
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" upload --bucket ${BUCKET_NAME} --local-file ${TEST_FILE_PATH} --object-name ${OBJECT_NAME}
info "Upload completed."

info "Waiting for logs to be flushed..."
sleep 2

info "--- Parsing Performance Metrics from Log (First Upload) ---"
# Use grep to find the line, then awk to extract the specific fields.
DEDUP_RATE_STR=$(grep "Successfully put object" ${LOG_FILE} | tail -1 | awk -F'dedupRate: ' '{print $2}' | awk '{print $1}')
COMPRESS_RATE=$(grep "Successfully put object" ${LOG_FILE} | tail -1 | awk -F'compressRate: ' '{print $2}' | awk '{print $1}')

info "Deduplication Rate (First Upload): ${DEDUP_RATE_STR}"
info "Compression Rate (First Upload): ${COMPRESS_RATE}"

info "--- Running Second Upload Test (for dedup) ---"
SECOND_OBJECT_NAME="${OBJECT_NAME}.2"
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" upload --bucket ${BUCKET_NAME} --local-file ${TEST_FILE_PATH} --object-name "${SECOND_OBJECT_NAME}"
info "Second upload completed."

info "Waiting for logs to be flushed..."
sleep 2

info "--- Parsing Performance Metrics from Log (Second Upload) ---"
DEDUP_RATE_2_STR=$(grep "Successfully put object" ${LOG_FILE} | tail -1 | awk -F'dedupRate: ' '{print $2}' | awk '{print $1}')
info "Deduplication Rate (Second Upload): ${DEDUP_RATE_2_STR}"

info "--- Running Download Test ---"
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" download --bucket ${BUCKET_NAME} --object-name ${OBJECT_NAME} --local-file ${DOWNLOADED_FILE_PATH}
info "Download completed."

info "--- Verifying Data Integrity ---"
DOWNLOADED_MD5=$(md5sum ${DOWNLOADED_FILE_PATH} | awk '{print $1}')
info "Downloaded file MD5: ${DOWNLOADED_MD5}"

if [ "${ORIGINAL_MD5}" != "${DOWNLOADED_MD5}" ]; then
    error "MD5 checksums do not match! Data corruption occurred."
else
    info "MD5 checksums match. Data integrity verified."
fi

info "--- Running Deletion and GC Test ---"
info "Deleting object ${OBJECT_NAME} from bucket ${BUCKET_NAME}..."
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" delete --bucket ${BUCKET_NAME} --object-name ${OBJECT_NAME}
info "Object deleted."

info "Deleting the second object ${OBJECT_NAME} from bucket ${BUCKET_NAME}..."
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" delete --bucket ${BUCKET_NAME} --object-name "${SECOND_OBJECT_NAME}"
info "Second object '${SECOND_OBJECT_NAME}' deleted."

info "Triggering Garbage Collection..."
${XCLI_CMD} gctrigger --redis-addr "${REDIS_ADDR}"
info "GC triggered. Waiting for it to run..."
sleep 15 # GC runs in the background, give it some time to process

info "Delete Bucket"
${XCLI_CMD} --endpoint "${GATEWAY_ENDPOINT}" rb ${BUCKET_NAME}

info "Verifying data cache cleanup..."
# The cache directory is mounted to ./test_data. After GC, it should be empty of DObj and manifest files.
FILE_COUNT=$(find ${TEST_DIR} -type f | wc -l)
if [ ${FILE_COUNT} -eq 0 ]; then
    info "Data cache is empty. GC test successful."
else
    error "Data cache is not empty. Found ${FILE_COUNT} files. GC test failed."
    ls -lR ${TEST_DIR}
fi

info "--- End-to-End Test Successful ---"