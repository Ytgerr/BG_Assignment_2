#!/bin/bash

echo "Starting Hadoop and Spark services..."

echo "cluster-slave-1" > $HADOOP_HOME/etc/hadoop/workers

$HADOOP_HOME/sbin/start-dfs.sh

$HADOOP_HOME/sbin/start-yarn.sh

mapred --daemon start historyserver

jps -lm

hdfs dfsadmin -report

hdfs dfsadmin -safemode leave

hdfs dfs -setrep -w 1 / 2>/dev/null

hdfs dfs -mkdir -p /user/root

hdfs dfs -mkdir -p /input/data
hdfs dfs -mkdir -p /indexer

scala -version 2>/dev/null

jps -lm

echo "All services started successfully!"
