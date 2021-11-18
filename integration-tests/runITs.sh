#!/bin/bash

echo "RDB READER component integration test"
docker-compose up -d
echo "Docker containers for ITs ready."
sleep 5
sh prepareTests.sh
echo "Preparation to tests OK"
echo "Integration tests start"
PROFILE=dev go test xqledger/rdbreader/apilogger -v
PROFILE=dev go test xqledger/rdbreader/configuration -v
PROFILE=dev go test xqledger/rdbreader/utils -v
PROFILE=dev go test xqledger/rdbreader/mongodb -v
PROFILE=dev go test xqledger/rdbreader/grpc -v 
echo "Integration tests complete"
echo "Cleaning up..."
cd ../integration-tests
docker-compose down
echo "Clean up complete. Bye!"