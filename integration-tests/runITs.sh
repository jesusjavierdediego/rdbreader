#!/bin/bash

echo "RDB READER component integration test"
docker-compose up -d
echo "Docker containers for ITs ready."
sleep 5
sh prepareTests.sh
echo "Preparation to tests OK"
echo "Integration tests start"
PROFILE=dev go test xqledger/rdbreader/apilogger -v 2>&1 | go-junit-report > ../testreports/apilogger.xml
PROFILE=dev go test xqledger/rdbreader/configuration -v 2>&1 | go-junit-report > ../testreports/configuration.xml
PROFILE=dev go test xqledger/rdbreader/utils -v 2>&1 | go-junit-report > ../testreports/utils.xml
PROFILE=dev go test xqledger/rdbreader/mongodb -v 2>&1 | go-junit-report > ../testreports/mongodb.xml
PROFILE=dev go test xqledger/rdbreader/grpc -v  2>&1 | go-junit-report > ../testreports/grpc.xml
echo "Integration tests complete"
echo "Cleaning up..."
cd ../integration-tests
docker-compose down
echo "Clean up complete. Bye!"