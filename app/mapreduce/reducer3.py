#!/usr/bin/env python3
import sys

docs = []
total_dl = 0
num_docs = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) < 4:
        continue

    tag = parts[0]
    if tag != "DOC_STAT":
        continue

    doc_id = parts[1]
    doc_title = parts[2]
    dl = int(parts[3])

    docs.append((doc_id, doc_title, dl))
    total_dl += dl
    num_docs += 1

for doc_id, doc_title, dl in docs:
    print(f"DOC\t{doc_id}\t{doc_title}\t{dl}")

if num_docs > 0:
    avg_dl = total_dl / num_docs
    print(f"CORPUS_STATS\t{num_docs}\t{avg_dl:.6f}\t{total_dl}")
