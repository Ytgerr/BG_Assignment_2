#!/usr/bin/env python3
import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster


def get_cassandra_data(query_terms):
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    rows = session.execute("SELECT num_docs, avg_dl, total_tokens FROM corpus_stats WHERE id = 1")
    corpus_row = rows.one()
    if not corpus_row:
        print("ERROR: No corpus statistics found in Cassandra!")
        cluster.shutdown()
        sys.exit(1)

    num_docs = corpus_row.num_docs
    avg_dl = corpus_row.avg_dl

    df_map = {}
    for term in query_terms:
        rows = session.execute(
            "SELECT term, df FROM vocabulary WHERE term = %s", (term,)
        )
        row = rows.one()
        if row:
            df_map[term] = row.df

    index_entries = []
    for term in query_terms:
        if term not in df_map:
            continue
        rows = session.execute(
            "SELECT term, doc_id, doc_title, tf, dl FROM term_index WHERE term = %s", (term,)
        )
        for row in rows:
            index_entries.append({
                'term': row.term,
                'doc_id': row.doc_id,
                'doc_title': row.doc_title,
                'tf': row.tf,
                'dl': row.dl
            })

    cluster.shutdown()
    return num_docs, avg_dl, df_map, index_entries


def compute_bm25():
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py <query>")
        sys.exit(1)

    query_text = " ".join(sys.argv[1:])
    print(f"\n{'='*50}")
    print(f"Search Query: {query_text}")
    print(f"{'='*50}\n")

    query_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', query_text.lower())
    query_terms = [t for t in query_clean.split() if len(t) > 1]
    query_terms = list(set(query_terms))

    if not query_terms:
        print("No valid query terms found after preprocessing.")
        sys.exit(0)

    print(f"Query terms: {query_terms}")

    print("Reading index data from Cassandra...")
    num_docs, avg_dl, df_map, index_entries = get_cassandra_data(query_terms)

    print(f"Corpus: N={num_docs}, avg_dl={avg_dl:.2f}")
    print(f"Terms found in index: {list(df_map.keys())}")
    print(f"Index entries retrieved: {len(index_entries)}")

    if not index_entries:
        print("\nNo documents found matching the query terms.")
        sys.exit(0)

    spark = SparkSession.builder \
        .appName('BM25 Search') \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    k1 = 1.2
    b = 0.75

    bc_num_docs = sc.broadcast(num_docs)
    bc_avg_dl = sc.broadcast(avg_dl)
    bc_df_map = sc.broadcast(df_map)
    bc_k1 = sc.broadcast(k1)
    bc_b = sc.broadcast(b)

    entries_rdd = sc.parallelize(index_entries)

    def compute_term_bm25(entry):
        term = entry['term']
        doc_id = entry['doc_id']
        doc_title = entry['doc_title']
        tf = entry['tf']
        dl = entry['dl']

        N = bc_num_docs.value
        avgdl = bc_avg_dl.value
        df_t = bc_df_map.value.get(term, 1)
        k = bc_k1.value
        b_val = bc_b.value

        idf = math.log(N / df_t) if df_t > 0 else 0

        numerator = (k + 1) * tf
        denominator = k * ((1 - b_val) + b_val * (dl / avgdl)) + tf

        bm25_score = idf * (numerator / denominator) if denominator > 0 else 0

        return ((doc_id, doc_title), bm25_score)

    bm25_rdd = entries_rdd \
        .map(compute_term_bm25) \
        .reduceByKey(lambda a, b_val: a + b_val)

    top_docs = bm25_rdd \
        .sortBy(lambda x: -x[1]) \
        .take(10)

    print(f"\n{'='*60}")
    print(f"Top 10 Relevant Documents for query: \"{query_text}\"")
    print(f"{'='*60}")
    print(f"{'Rank':<6}{'Doc ID':<12}{'BM25 Score':<15}{'Title'}")
    print(f"{'-'*60}")

    if not top_docs:
        print("No relevant documents found.")
    else:
        for rank, ((doc_id, doc_title), score) in enumerate(top_docs, 1):
            print(f"{rank:<6}{doc_id:<12}{score:<15.4f}{doc_title}")

    print(f"{'='*60}\n")

    spark.stop()


if __name__ == "__main__":
    compute_bm25()
