#!/bin/bash
sh delete-files.sh
cd ..
cd docker || return
docker-compose down
cd .. || return
sh cancel-job.sh
stop-cluster.sh