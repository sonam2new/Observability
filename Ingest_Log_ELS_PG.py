import re
import psycopg2
from elasticsearch import Elasticsearch
import time
from passlib.hash import sha256_crypt
import getpass

# Log entry regex pattern
log_pattern = r"(\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(.*)"

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
es_index = "hdfs_log"

username = input("Enter username")
password = getpass.getpass("Enter password")

#Initialize PostgreSQL connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='logs_db',
    user=username,
    password=sha256_crypt.hash(password)
)

def parse_log(log_entry):
    """
    Parse a log and extract the relevant fields.
    Returns a tuple of timestamp, log level, and log message.
    """
    match = re.match(log_pattern, log_entry)
    if match:
        timestamp = match.group(1)
        log_level = match.group(4)
        log_message = match.group(5)
        return timestamp, log_level, log_message
    else:
        return None

def store_log_in_elasticsearch(timestamp, log_level, log_message):
    """
    Store a log entry in Elasticsearch.
    """
    doc = {
        "timestamp": timestamp,
        "log_level": log_level,
        "log_message": log_message
    }
    es.index(index=es_index, body=doc)

def store_log_in_postgresql(timestamp, log_level, log_message):
    """
    Store a log entry in PostgreSQL.
    """
    cursor = conn.cursor()
    query = "INSERT INTO logs (timestamp, log_level, log_message) VALUES (%s, %s, %s)"
    cursor.execute(query, (timestamp, log_level, log_message))
    conn.commit()
    cursor.close()

# Elasticsearch mapping for the logs index
mapping = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "log_level": {"type": "keyword"},
            "log_message": {"type": "text"}
        }
    }
}

# Create the logs index with the mapping
es.indices.create(index=es_index, body=mapping)

# Open the log file
log_file_path = "/home/sonamsingh/Downloads/HDFS_2k.log"
with open(log_file_path, "r") as log_file:
    #  Start time for Elasticsearch ingestion
    start_time_es = time.time()

    # Track the number of logs ingested into Elasticsearch
    es_logs_ingested = 0

    # Process each line in the log file
    for log_entry in log_file:
        parsed_entry = parse_log(log_entry)
        if parsed_entry:
            timestamp, log_level, log_message = parsed_entry
            store_log_in_elasticsearch(timestamp, log_level, log_message)
            es_logs_ingested += 1
            store_log_in_postgresql(timestamp, log_level, log_message)

# Calculate the ingestion time for Elasticsearch
execution_time_es = time.time() - start_time_es

# Get the disk size occupied by the Elasticsearch index
index_stats = es.indices.stats(index=es_index)
disk_size_es = index_stats["indices"][es_index]["total"]["store"]["size_in_bytes"]

# Get the number of logs ingested into PostgreSQL
cursor = conn.cursor()
select_query = "SELECT COUNT(*) FROM logs"
cursor.execute(select_query)
logs_ingested_pg = cursor.fetchone()[0]

# Close the PostgreSQL connection
conn.close()

# Print the ingestion results
print("Logs ingested in Elasticsearch: {}".format(es_logs_ingested))
print("Logs ingested in PostgreSQL: {}".format(logs_ingested_pg))
print("Ingestion Time - Elasticsearch: {:.2f} seconds".format(execution_time_es))
print("Disk Size - Elasticsearch: {:.2f} bytes".format(disk_size_es))

