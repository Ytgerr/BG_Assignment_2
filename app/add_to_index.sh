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

hdfs dfs -mkdir -p /data
hdfs dfs -put -f "$FILE_PATH" /data/

echo "Removing old entry for doc_id=$DOC_ID from existing index input (if any)..."
MAIN_INPUT="/input/data/part-00000"
if hdfs dfs -test -e "$MAIN_INPUT" 2>/dev/null; then
    hdfs dfs -cat "$MAIN_INPUT" | grep -v "^${DOC_ID}	" > /tmp/filtered_input.txt
    hdfs dfs -put -f /tmp/filtered_input.txt "$MAIN_INPUT"
    rm -f /tmp/filtered_input.txt
fi

hdfs dfs -rm -f "/input/data/new_doc_${DOC_ID}" 2>/dev/null

TEMP_INPUT="/tmp/new_doc_input"
hdfs dfs -rm -r "$TEMP_INPUT" 2>/dev/null

printf '%s\t%s\t%s\n' "$DOC_ID" "$DOC_TITLE" "$DOC_TEXT" > /tmp/new_doc_line.txt
hdfs dfs -mkdir -p "$TEMP_INPUT"
hdfs dfs -put /tmp/new_doc_line.txt "$TEMP_INPUT/part-00000"
rm -f /tmp/new_doc_line.txt

hdfs dfs -cp "$TEMP_INPUT/part-00000" "/input/data/new_doc_${DOC_ID}"
hdfs dfs -rm -r "$TEMP_INPUT" 2>/dev/null

echo "Document added to HDFS input."

echo "Re-indexing all documents..."
bash index.sh /input/data

echo "=========================================="
echo "Document added and index updated!"
echo "=========================================="
