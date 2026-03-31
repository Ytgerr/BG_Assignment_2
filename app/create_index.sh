#!/bin/bash

INPUT_PATH=${1:-/input/data}

echo "=========================================="
echo "Creating index using MapReduce pipelines"
echo "Input path: $INPUT_PATH"
echo "=========================================="

hdfs dfs -rm -r /indexer 2>/dev/null
hdfs dfs -rm -r /tmp/indexer 2>/dev/null

hdfs dfs -mkdir -p /indexer
hdfs dfs -mkdir -p /tmp/indexer

STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar"
if [ ! -f "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find $HADOOP_HOME/share/hadoop/tools/lib -name "hadoop-streaming-*.jar" | head -1)
fi
echo "Using streaming jar: $STREAMING_JAR"

echo ""
echo "=========================================="
echo "Pipeline 1: Computing Term Frequencies"
echo "=========================================="
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D dfs.replication=1 \
    -input "$INPUT_PATH" \
    -output /indexer/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 1 failed!"
    exit 1
fi

echo "Pipeline 1 completed. Checking output..."
hdfs dfs -ls /indexer/index
echo "Sample output from Pipeline 1:"
hdfs dfs -cat /indexer/index/part-00000 | head -10

echo ""
echo "=========================================="
echo "Pipeline 2: Computing Document Frequencies"
echo "=========================================="
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D dfs.replication=1 \
    -input /indexer/index \
    -output /indexer/vocab \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 2 failed!"
    exit 1
fi

echo "Pipeline 2 completed. Checking output..."
hdfs dfs -ls /indexer/vocab
echo "Sample output from Pipeline 2:"
hdfs dfs -cat /indexer/vocab/part-00000 | head -10

echo ""
echo "=========================================="
echo "Pipeline 3: Computing Document Statistics"
echo "=========================================="
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D dfs.replication=1 \
    -input "$INPUT_PATH" \
    -output /indexer/stats \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -file /app/mapreduce/mapper3.py \
    -file /app/mapreduce/reducer3.py

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 3 failed!"
    exit 1
fi

echo "Pipeline 3 completed. Checking output..."
hdfs dfs -ls /indexer/stats
echo "Sample output from Pipeline 3:"
hdfs dfs -cat /indexer/stats/part-00000 | head -10

echo ""
echo "=========================================="
echo "Index creation completed successfully!"
echo "=========================================="
echo "Index data stored in HDFS:"
echo "  /indexer/index  - Term frequencies (term, doc_id, doc_title, tf, dl)"
echo "  /indexer/vocab  - Document frequencies (term, df)"
echo "  /indexer/stats  - Document statistics (doc info + corpus stats)"
hdfs dfs -ls /indexer/
