#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) < 5:
        continue

    term = parts[0]
    print(f"{term}\t1")
