#!/bin/bash

echo "=========================================="
echo "Storing index data in Cassandra/ScyllaDB"
echo "=========================================="

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

python3 /app/app.py

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to store index in Cassandra!"
    exit 1
fi

echo "=========================================="
echo "Index stored in Cassandra/ScyllaDB successfully!"
echo "=========================================="
