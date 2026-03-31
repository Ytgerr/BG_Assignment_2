#!/usr/bin/env python3
import os
import re
import subprocess
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

sc = spark.sparkContext

data_dir = "data"
files = [f for f in os.listdir(data_dir) if f.endswith('.txt')]
print(f"Found {len(files)} documents in {data_dir}/")

lines = []
for filename in sorted(files):
    name_without_ext = filename[:-4]
    parts = name_without_ext.split('_', 1)
    if len(parts) < 2:
        continue
    doc_id = parts[0]
    doc_title = parts[1].replace('_', ' ')

    filepath = os.path.join(data_dir, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception as e:
        print(f"  Warning: Could not read {filename}: {e}")
        continue

    if not text.strip():
        continue

    clean_text = re.sub(r'[\t\n\r]+', ' ', text).strip()
    clean_title = re.sub(r'[\t\n\r]+', ' ', doc_title).strip()
    lines.append(doc_id + "\t" + clean_title + "\t" + clean_text)

print(f"Prepared {len(lines)} document entries for MapReduce input.")

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], capture_output=True)

input_rdd = sc.parallelize(lines, 1)
input_rdd.saveAsTextFile("/input/data")

spark.stop()
print("Data preparation completed successfully!")
