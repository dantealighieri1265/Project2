#!/bin/bash
cd ..
JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
export JOB_ID
flink cancel "$JOB_ID"
flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
gnome-terminal -- bash -c "echo Query1OutputMonthly;kafka-console-consumer.sh --topic query1_monthly_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query1OutputWeekly;kafka-console-consumer.sh --topic query1_weekly_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"

gnome-terminal -- bash -c "echo Query2OutputMonthly;kafka-console-consumer.sh --topic query2_monthly_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query2OutputWeekly;kafka-console-consumer.sh --topic query2_weekly_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"

gnome-terminal -- bash -c "echo Query3OutputOneHour;kafka-console-consumer.sh --topic query3_one_hour_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query3OutputTwoHour;kafka-console-consumer.sh --topic query3_two_hour_output --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
sleep 10
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer