#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue

    doc_id = parts[0].strip()
    doc_title = parts[1].strip()
    doc_text = parts[2].strip()

    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', doc_text.lower())
    tokens = text.split()

    dl = len(tokens)

    if dl == 0:
        continue

    for token in tokens:
        token = token.strip()
        if token and len(token) > 1:
            print(f"{token}\t{doc_id}\t{doc_title}\t1\t{dl}")
