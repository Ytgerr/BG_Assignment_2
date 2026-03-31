#!/bin/bash

echo "=========================================="
echo "Adding document to index"
echo "=========================================="

if [ -z "$1" ]; then
    echo "Usage: bash add_to_index.sh <path_to_file>"
    echo "File should be named as: <doc_id>_<doc_title>.txt"
    exit 1
fi

FILE_PATH="$1"
FILENAME=$(basename "$FILE_PATH")

if [ ! -f "$FILE_PATH" ]; then
    echo "ERROR: File not found: $FILE_PATH"
    exit 1
fi

echo "File: $FILENAME"

DOC_ID=$(echo "$FILENAME" | cut -d'_' -f1)
DOC_TITLE=$(echo "$FILENAME" | sed "s/^${DOC_ID}_//" | sed 's/\.txt$//' | sed 's/_/ /g')

echo "Doc ID: $DOC_ID"
echo "Doc Title: $DOC_TITLE"

DOC_TEXT=$(cat "$FILE_PATH" | tr '\t\n\r' '   ')

if [ -z "$DOC_TEXT" ]; then
    echo "ERROR: File is empty!"
    exit 1
fi

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -put -f "$FILE_PATH" /data/

TEMP_INPUT="/tmp/new_doc_input"
hdfs dfs -rm -r "$TEMP_INPUT" 2>/dev/null

echo -e "${DOC_ID}\t${DOC_TITLE}\t${DOC_TEXT}" > /tmp/new_doc_line.txt
hdfs dfs -put /tmp/new_doc_line.txt "$TEMP_INPUT/part-00000"

hdfs dfs -cp "$TEMP_INPUT/part-00000" "/input/data/new_doc_${DOC_ID}"

echo "Document added to HDFS input."

echo "Re-indexing all documents..."
bash index.sh /input/data

echo "=========================================="
echo "Document added and index updated!"
echo "=========================================="
