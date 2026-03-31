# Simple Search Engine using Hadoop MapReduce

## Assignment 2 — Big Data Course (IU)

**Author:** Andrei Zhdanov

---

## 1. Methodology

### 1.1 System Architecture

The search engine follows a three-tier architecture:

1. **Data Preparation** (PySpark) — reads raw documents, cleans text, and uploads consolidated input to HDFS.
2. **Indexing** (Hadoop MapReduce + Cassandra) — three MapReduce pipelines compute term frequencies, document frequencies, and corpus statistics; results are stored in Cassandra.
3. **Ranking** (PySpark + Cassandra) — retrieves index data from Cassandra, computes BM25 scores via Spark RDD operations, and returns the top-10 most relevant documents.

### 1.2 Data Preparation

`prepare_data.py` uses PySpark to:

- Read 1 000 Wikipedia articles stored as individual `.txt` files in `data/`.
- Build a single tab-separated file in HDFS (`/input/data`) with the format `<doc_id>\t<doc_title>\t<doc_text>`.
- Preprocessing: tabs and newlines are replaced with spaces; filenames are sanitized.

### 1.3 Indexing Pipeline (MapReduce)

#### Pipeline 1 — Term Frequency (TF)

| Stage | File | Input → Output |
|-------|------|----------------|
| Mapper | `mapper1.py` | `<doc_id>\t<title>\t<text>` → `<term>\t<doc_id>\t<title>\t1\t<dl>` |
| Reducer | `reducer1.py` | aggregates counts → `<term>\t<doc_id>\t<title>\t<tf>\t<dl>` |

Result stored in `/indexer/index`.

#### Pipeline 2 — Document Frequency (DF)

| Stage | File | Input → Output |
|-------|------|----------------|
| Mapper | `mapper2.py` | Pipeline 1 output → `<term>\t1` |
| Reducer | `reducer2.py` | aggregates → `<term>\t<df>` |

Result stored in `/indexer/vocab`.

#### Pipeline 3 — Document & Corpus Statistics

| Stage | File | Input → Output |
|-------|------|----------------|
| Mapper | `mapper3.py` | raw input → `DOC_STAT\t<doc_id>\t<title>\t<dl>` |
| Reducer | `reducer3.py` | collects all docs → per-doc stats + `CORPUS_STATS\t<N>\t<avg_dl>\t<total>` |

Result stored in `/indexer/stats`.

### 1.4 Cassandra Storage Schema

Keyspace: `search_engine`. Four tables:

| Table | Primary Key | Columns | Purpose |
|-------|-------------|---------|---------|
| `term_index` | `(term, doc_id)` | `doc_title`, `tf`, `dl` | Inverted index |
| `vocabulary` | `term` | `df` | Document frequency per term |
| `doc_stats` | `doc_id` | `doc_title`, `dl` | Document metadata |
| `corpus_stats` | `id` | `num_docs`, `avg_dl`, `total_tokens` | Global corpus info |

Design rationale:

- `term_index` uses composite PK `(term, doc_id)` — partition by term enables efficient per-term lookups.
- Batch inserts (batch size 50) for throughput.

### 1.5 BM25 Ranking

The ranking engine (`query.py`) implements BM25:

$$BM25(q, d) = \sum_{t \in q} \log\!\left(\frac{N}{df(t)}\right) \cdot \frac{(k_1 + 1) \cdot tf(t, d)}{k_1 \cdot \left[(1 - b) + b \cdot \frac{dl(d)}{avg\_dl}\right] + tf(t, d)}$$

Parameters: `k1 = 1.2`, `b = 0.75`.

Steps:

1. Tokenize query (same preprocessing as indexer).
2. Retrieve index data from Cassandra.
3. Create a PySpark RDD from index entries.
4. `map()` — compute per-term BM25 contribution for each (term, doc) pair.
5. `reduceByKey()` — aggregate scores per document.
6. Sort descending, take top 10.

### 1.6 Adding Documents

`add_to_index.sh` allows adding a single document on the fly:

1. Extracts doc ID and title from the filename.
2. Uploads the file to HDFS.
3. Re-runs the full indexing pipeline.

---

## 2. How to Run

### 2.1 Prerequisites

- Docker & Docker Compose installed.
- 1 000 document files already present in `app/data/` (or generate them with `generate_data.py`).

### 2.2 Launch

```bash
docker compose up
```

This will automatically:

1. Start Hadoop (master + slave) and Cassandra containers.
2. Install Python dependencies.
3. Prepare data in HDFS.
4. Run three MapReduce indexing pipelines.
5. Store the index in Cassandra.
6. Execute three sample search queries.

### 2.3 Interactive Queries

```bash
docker exec -it cluster-master bash -c "cd /app && bash search.sh 'your query here'"
```

### 2.4 Sample Queries

| Query | Expected results |
|-------|-----------------|
| `machine learning algorithms` | Documents about ML, AI, computational algorithms |
| `history of ancient civilizations` | Documents about historical civilizations (rare terms like "civilizations" boost IDF) |
| `climate change effects` | Documents about environmental science, ecology |

---

## 3. Component Summary

| Component | Files | Technology |
|-----------|-------|------------|
| Data Preparation | `prepare_data.py`, `prepare_data.sh` | PySpark |
| TF Computation | `mapper1.py`, `reducer1.py` | Hadoop Streaming |
| DF Computation | `mapper2.py`, `reducer2.py` | Hadoop Streaming |
| Doc Statistics | `mapper3.py`, `reducer3.py` | Hadoop Streaming |
| Index Storage | `app.py`, `store_index.sh` | Python cassandra-driver |
| BM25 Ranking | `query.py`, `search.sh` | PySpark RDD |
| Orchestration | `app.sh`, `index.sh`, `create_index.sh` | Bash |

---

## References

1. Robertson, S., & Zaragoza, H. (2009). *The Probabilistic Relevance Framework: BM25 and Beyond.* Foundations and Trends in Information Retrieval.
2. <https://en.wikipedia.org/wiki/Okapi_BM25>
3. <https://www.ai-bites.net/tf-idf-and-bm25-for-rag-a-complete-guide/>
