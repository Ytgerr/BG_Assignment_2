#!/usr/bin/env python3
import os
import sys

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("PySpark is not installed. Install it with: pip install pyspark")
    sys.exit(1)

from pathvalidate import sanitize_filename

parquet_path = None
for candidate in ["app/p.parquet", "p.parquet"]:
    if os.path.exists(candidate):
        parquet_path = candidate
        break

if parquet_path is None:
    print("ERROR: No parquet file found!")
    print("Please download a.parquet from:")
    print("  https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=a.parquet")
    print("And place it in the app/ directory or current directory.")
    sys.exit(1)

print(f"Reading parquet file: {parquet_path}")

spark = SparkSession.builder \
    .appName('data generation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

df = spark.read.parquet(parquet_path)
df = df.select(['id', 'title', 'text'])

total = df.count()
print(f"Total articles in parquet: {total}")

n = 1000
fraction = min(1.0, 100 * n / total)
df = df.sample(fraction=fraction, seed=0).limit(n)

data_dir = os.path.join("app", "data")
os.makedirs(data_dir, exist_ok=True)

collected = df.collect()
count = 0
for row in collected:
    doc_id = str(row['id'])
    title = row['title'] if row['title'] else "untitled"
    text = row['text'] if row['text'] else ""

    if not text.strip():
        continue

    safe_title = sanitize_filename(title).replace(" ", "_")
    filename = sanitize_filename(doc_id + "_" + safe_title) + ".txt"
    filepath = os.path.join(data_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)

    count += 1
    if count % 100 == 0:
        print(f"  Created {count} documents...")

spark.stop()
print(f"\nDone! Created {count} documents in {data_dir}/")
print("You can now push the app/data/ folder to GitHub.")
