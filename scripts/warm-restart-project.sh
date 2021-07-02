#!/bin/bash
cd ..
export JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
flink cancel $JOB_ID
flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer