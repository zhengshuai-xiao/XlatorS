#!/bin/bash

# #############################################################################
#
# Redis Management Script
#
# This script starts, stops, or restarts the Redis server.
#
# #############################################################################

# --- Redis Configuration ---
REDIS_CONF_FILE="/etc/redis/redis.conf"

# --- Functions ---

start_redis() {
    echo "Starting Redis..."
    if pgrep -f "redis-server" > /dev/null; then
        echo "Redis is already running."
        return 0
    fi

    # Start Redis using the specified configuration file
    nohup redis-server "$REDIS_CONF_FILE" > /dev/null 2>&1 &

    # Check if Redis started successfully
    sleep 3
    if pgrep -f "redis-server" > /dev/null; then
        echo "Redis started successfully."
    else
        echo "Failed to start Redis."
        return 1
    fi
}

stop_redis() {
    echo "Stopping Redis..."
    if pgrep -f "redis-server" > /dev/null; then
        # Use redis-cli for a graceful shutdown
        redis-cli shutdown > /dev/null 2>&1
        sleep 2
        # If graceful shutdown fails, use pkill as a fallback
        if pgrep -f "redis-server" > /dev/null; then
            pkill -f "redis-server"
        fi
        echo "Redis stopped."
    else
        echo "Redis is not running."
    fi
}

# --- Main Program ---
# This block will only run if the script is executed directly, not if it's sourced.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    case "$1" in
        start)
            start_redis
            ;;
        stop)
            stop_redis
            ;;
        restart)
            stop_redis
            sleep 2
            start_redis
            ;;
        *)
            echo "Usage: $0 {start|stop|restart}"
            exit 1
            ;;
    esac
fi
