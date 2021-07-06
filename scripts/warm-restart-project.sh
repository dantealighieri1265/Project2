#!/bin/bash
cd ..
JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
export JOB_ID
flink cancel "$JOB_ID"
cp flink-config/flink-conf.yaml "$FLINK_HOME/conf/flink-conf.yam"
flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
sleep 10
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer