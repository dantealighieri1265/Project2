#!/bin/bash
cd ..
mvn clean package #da rimuovere
cd docker || return
docker-compose up -d
cd ..
cp flink-config/flink-conf.yaml "$FLINK_HOME/conf/flink-conf.yaml"
sleep 5
start-cluster.sh
kafka-topics.sh --create --topic query --zookeeper localhost:2181 --replication-factor 2 --partitions 1

kafka-topics.sh --create --topic query1_monthly_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1
kafka-topics.sh --create --topic query1_weekly_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1

kafka-topics.sh --create --topic query2_monthly_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1
kafka-topics.sh --create --topic query2_weekly_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1

kafka-topics.sh --create --topic query3_one_hour_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1
kafka-topics.sh --create --topic query3_two_hour_output --zookeeper localhost:2181 --replication-factor 2 --partitions 1


flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
gnome-terminal -- bash -c "echo Query1OutputMonthly;kafka-console-consumer.sh --topic query1_monthly_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query1OutputWeekly;kafka-console-consumer.sh --topic query1_weekly_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"

gnome-terminal -- bash -c "echo Query2OutputMonthly;kafka-console-consumer.sh --topic query2_monthly_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query2OutputWeekly;kafka-console-consumer.sh --topic query2_weekly_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"

gnome-terminal -- bash -c "echo Query3OutputOneHour;kafka-console-consumer.sh --topic query3_one_hour_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"
gnome-terminal -- bash -c "echo Query3OutputTwoHour;kafka-console-consumer.sh --topic query3_two_hour_output --from-beginning --bootstrap-server localhost:9092,localhost:9093,localhost:9094;exec bash"

sleep 10
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer

