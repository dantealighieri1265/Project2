#!/bin/bash
sh cancel-job.sh
sh delete-files.sh
kill $(cat ../flink-config/pid_values)
cd ..
cd docker || return
docker-compose down
cd .. || return
stop-cluster.sh