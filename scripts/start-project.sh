#!/bin/bash
cd ..
cd docker
docker-compose up -d
cd ..
cp flink-config/flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml
sleep 5
start-cluster.sh
kafka-topics.sh --create --topic query --bootstrap-server localhost:9092
flink -D taskmanager.numberOfTaskSlots: 12
flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
sleep 5
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer