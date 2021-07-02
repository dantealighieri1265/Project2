#!/bin/bash
cd ..
cd docker
docker-compose down
cd ..
export JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
flink cancel $JOB_ID
stop-cluster.sh