#!/usr/bin/env python3
import subprocess
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement


def wait_for_cassandra(host='cassandra-server', port=9042, max_retries=30, delay=10):
    for i in range(max_retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            print(f"Cassandra is ready! (attempt {i+1})")
            return cluster, session
        except Exception as e:
            print(f"Waiting for Cassandra... (attempt {i+1}/{max_retries}): {e}")
            time.sleep(delay)
    raise Exception("Cassandra is not available after maximum retries")


def read_hdfs_file(path):
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", path],
        capture_output=True, text=True, encoding='utf-8'
    )
    if result.returncode != 0:
        print(f"Error reading {path}: {result.stderr}")
        return []
    return [line for line in result.stdout.strip().split('\n') if line.strip()]


def main():
    print("Connecting to Cassandra...")
    cluster, session = wait_for_cassandra()

    print("Creating keyspace 'search_engine'...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('search_engine')

    print("Dropping existing tables...")
    session.execute("DROP TABLE IF EXISTS term_index")
    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS doc_stats")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    print("Creating tables...")

    session.execute("""
        CREATE TABLE IF NOT EXISTS term_index (
            term text,
            doc_id text,
            doc_title text,
            tf int,
            dl int,
            PRIMARY KEY (term, doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            doc_title text,
            dl int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            id int PRIMARY KEY,
            num_docs int,
            avg_dl double,
            total_tokens bigint
        )
    """)

    print("Loading term index data from HDFS...")
    index_lines = read_hdfs_file("/indexer/index/part-00000")
    print(f"  Found {len(index_lines)} term-document entries")

    insert_index = session.prepare(
        "INSERT INTO term_index (term, doc_id, doc_title, tf, dl) VALUES (?, ?, ?, ?, ?)"
    )

    batch_size = 50
    count = 0
    batch = BatchStatement()
    for line in index_lines:
        parts = line.split('\t')
        if len(parts) < 5:
            continue
        term = parts[0]
        doc_id = parts[1]
        doc_title = parts[2]
        tf = int(parts[3])
        dl = int(parts[4])
        batch.add(insert_index, (term, doc_id, doc_title, tf, dl))
        count += 1
        if count % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement()
            if count % 10000 == 0:
                print(f"  Inserted {count} term-document entries...")

    if count % batch_size != 0:
        session.execute(batch)
    print(f"  Total term-document entries inserted: {count}")

    print("Loading vocabulary data from HDFS...")
    vocab_lines = read_hdfs_file("/indexer/vocab/part-00000")
    print(f"  Found {len(vocab_lines)} vocabulary entries")

    insert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )

    batch = BatchStatement()
    count = 0
    for line in vocab_lines:
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        term = parts[0]
        df = int(parts[1])
        batch.add(insert_vocab, (term, df))
        count += 1
        if count % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement()
            if count % 10000 == 0:
                print(f"  Inserted {count} vocabulary entries...")

    if count % batch_size != 0:
        session.execute(batch)
    print(f"  Total vocabulary entries inserted: {count}")

    print("Loading document statistics from HDFS...")
    stats_lines = read_hdfs_file("/indexer/stats/part-00000")

    insert_doc = session.prepare(
        "INSERT INTO doc_stats (doc_id, doc_title, dl) VALUES (?, ?, ?)"
    )
    insert_corpus = session.prepare(
        "INSERT INTO corpus_stats (id, num_docs, avg_dl, total_tokens) VALUES (?, ?, ?, ?)"
    )

    batch = BatchStatement()
    doc_count = 0
    for line in stats_lines:
        parts = line.split('\t')
        if len(parts) < 2:
            continue

        if parts[0] == "DOC" and len(parts) >= 4:
            doc_id = parts[1]
            doc_title = parts[2]
            dl = int(parts[3])
            batch.add(insert_doc, (doc_id, doc_title, dl))
            doc_count += 1
            if doc_count % batch_size == 0:
                session.execute(batch)
                batch = BatchStatement()
                if doc_count % 5000 == 0:
                    print(f"  Inserted {doc_count} document stats...")

        elif parts[0] == "CORPUS_STATS" and len(parts) >= 4:
            num_docs = int(parts[1])
            avg_dl = float(parts[2])
            total_tokens = int(parts[3])
            if doc_count % batch_size != 0:
                session.execute(batch)
                batch = BatchStatement()
            session.execute(insert_corpus, (1, num_docs, avg_dl, total_tokens))
            print(f"  Corpus stats: N={num_docs}, avg_dl={avg_dl:.2f}, total_tokens={total_tokens}")

    if doc_count % batch_size != 0:
        session.execute(batch)
    print(f"  Total document stats inserted: {doc_count}")

    print("\n==========================================")
    print("Verification:")
    rows = session.execute("SELECT COUNT(*) FROM term_index")
    print(f"  term_index entries: {rows.one()[0]}")
    rows = session.execute("SELECT COUNT(*) FROM vocabulary")
    print(f"  vocabulary entries: {rows.one()[0]}")
    rows = session.execute("SELECT COUNT(*) FROM doc_stats")
    print(f"  doc_stats entries: {rows.one()[0]}")
    rows = session.execute("SELECT * FROM corpus_stats WHERE id = 1")
    row = rows.one()
    if row:
        print(f"  corpus_stats: num_docs={row.num_docs}, avg_dl={row.avg_dl:.2f}, total_tokens={row.total_tokens}")
    print("==========================================")

    cluster.shutdown()
    print("Done storing index in Cassandra!")


if __name__ == "__main__":
    main()
