#!/bin/bash

REDIS_CONTAINER_ID=$(docker run -p 6379:6379 -d redis:5.0-alpine)

if [ -z $REDIS_CONTAINER_ID ]; then
    echo "Could not start Redis in Docker"
    exit 1
fi

echo "Redis Container Id: $REDIS_CONTAINER_ID"

echo "Wait 3 seconds for Redis"
sleep 5

echo "===================================="
echo "=== Test with THREADED scheduler ==="
echo "===================================="
cargo run --release

TEST_EXIT_CODE_THREADED=$?
if [ $TEST_EXIT_CODE_THREADED -eq 0 ]; then
    echo "Integration test succeeded"
else
    echo "Integration test failed"
fi

echo "================================="
echo "=== Test with BASIC scheduler ==="
echo "================================="
cargo run --release --features "basic_scheduler"

TEST_EXIT_CODE_BASIC=$?
if [ $TEST_EXIT_CODE_BASIC -eq 0 ]; then
    echo "Integration test succeeded"
else
    echo "Integration test failed"
fi

echo "Removing Redis Container: $REDIS_CONTAINER_ID"
docker rm -f $REDIS_CONTAINER_ID

echo "Test completed"

if [ $TEST_EXIT_CODE_BASIC -eq 0 ] && [ $TEST_EXIT_CODE_THREADED -eq 0 ]; then
    echo "SUCCESS"
else
    echo "FAILURE"
    exit 1
fi
