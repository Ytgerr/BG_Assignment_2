#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

DOC_COUNT=$(ls data/*.txt 2>/dev/null | wc -l)
echo "Found $DOC_COUNT documents in data/ folder."

if [ "$DOC_COUNT" -eq 0 ]; then
    echo "ERROR: No documents found in data/ folder!"
    exit 1
fi

hdfs dfs -rm -r /input/data 2>/dev/null
hdfs dfs -rm -r /data 2>/dev/null

echo "Running data preparation with PySpark..."
spark-submit prepare_data.py

if [ $? -ne 0 ]; then
    echo "ERROR: Data preparation failed!"
    exit 1
fi

echo "Verifying data in HDFS..."
echo "Input data in /input/data:"
hdfs dfs -ls /input/data

echo "Done data preparation!"
