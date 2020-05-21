#!/bin/bash

REDIS_CONTAINER_ID=$(docker run -p 6379:6379 -d redis:4.0-alpine)

if [ -z $REDIS_CONTAINER_ID ]; then
    echo "Could not start Redis in Docker"
    exit 1
fi

echo "Redis Container Id: $REDIS_CONTAINER_ID"

echo "Wait 5 seconds for Redis"
sleep 5

cargo run --release

TEST_EXIT_CODE=$?
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "Integration test succeeded"
else
    echo "Integration test failed"
fi

echo "Removing Redis Container: $REDIS_CONTAINER_ID"
docker rm -f $REDIS_CONTAINER_ID

echo "Test completed"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "SUCCESS"
else
    echo "FAILURE"
    exit $TEST_EXIT_CODE
fi
