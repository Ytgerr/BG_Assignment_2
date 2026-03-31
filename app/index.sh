#!/bin/bash

INPUT_PATH=${1:-/input/data}

echo "=========================================="
echo "Running full indexing pipeline"
echo "Input path: $INPUT_PATH"
echo "=========================================="

echo ""
echo "Step 1: Creating index with MapReduce..."
bash create_index.sh "$INPUT_PATH"

if [ $? -ne 0 ]; then
    echo "ERROR: Index creation failed!"
    exit 1
fi

echo ""
echo "Step 2: Storing index in Cassandra/ScyllaDB..."
bash store_index.sh

if [ $? -ne 0 ]; then
    echo "ERROR: Storing index failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Full indexing pipeline completed!"
echo "=========================================="
