#!/bin/bash
cd ..
cd docker
docker-compose up -d
cd ..
sleep 5
start-cluster.sh
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer
flink run --parallelism 1 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar