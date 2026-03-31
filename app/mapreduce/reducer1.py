#!/usr/bin/env python3
import sys

current_key = None
current_term = None
current_doc_id = None
current_doc_title = None
current_tf = 0
current_dl = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) < 5:
        continue

    term = parts[0]
    doc_id = parts[1]
    doc_title = parts[2]
    count = int(parts[3])
    dl = int(parts[4])

    key = (term, doc_id)

    if current_key == key:
        current_tf += count
    else:
        if current_key is not None:
            print(f"{current_term}\t{current_doc_id}\t{current_doc_title}\t{current_tf}\t{current_dl}")
        current_key = key
        current_term = term
        current_doc_id = doc_id
        current_doc_title = doc_title
        current_tf = count
        current_dl = dl

if current_key is not None:
    print(f"{current_term}\t{current_doc_id}\t{current_doc_title}\t{current_tf}\t{current_dl}")
