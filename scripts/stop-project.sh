#!/bin/bash
sh delete-directories.sh
cd ..
cd docker || return
docker-compose down
cd .. || return
JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
export JOB_ID
flink cancel "$JOB_ID"
stop-cluster.sh