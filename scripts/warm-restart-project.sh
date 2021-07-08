#!/bin/bash
sh delete-files.sh
sh cancel-job.sh
cd ..
cp flink-config/flink-conf.yaml "$FLINK_HOME/conf/flink-conf.yaml"
gnome-terminal --geometry=50x15+1+540 -- bash -c "echo Consumer. CTRL+C to stop.;java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Consumer;exec bash"

flink run --parallelism 12 -d --class "queries.QueriesStart" ~/Scrivania/Project2/target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar
echo "Waiting start producer..."
sleep 10
java -cp target/SABD_Project2-0.0.1-SNAPSHOT-jar-with-dependencies.jar utils.Producer