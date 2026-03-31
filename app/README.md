## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. It contains 1000 documents extracted from `a.parquet` file from the Wikipedia dataset. Each document is named `<doc_id>_<doc_title>.txt`.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.
- Pipeline 1 (`mapper1.py`, `reducer1.py`): Computes term frequencies (TF) per document
- Pipeline 2 (`mapper2.py`, `reducer2.py`): Computes document frequencies (DF) per term
- Pipeline 3 (`mapper3.py`, `reducer3.py`): Computes document statistics (doc lengths, corpus stats)

### app.py
Python script to store index data from HDFS into Cassandra/ScyllaDB tables.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
PySpark script that reads documents from the data/ folder and creates consolidated input in HDFS /input/data for MapReduce processing.

### prepare_data.sh
Shell script that uploads documents to HDFS and runs prepare_data.py.

### query.py
PySpark application that reads a user's query, retrieves index data from Cassandra, computes BM25 scores, and returns the top 10 relevant documents.

### requirements.txt
Python dependencies needed for running the programs.

### search.sh
Script to run the query.py PySpark app on Hadoop YARN cluster.

### start-services.sh
Script to initiate Hadoop and Spark services. Called from app.sh.

### store_index.sh
Script to load index data from HDFS into Cassandra/ScyllaDB tables.

### add_to_index.sh
Optional script to add a single document to the index.
