import re
import psycopg2
from elasticsearch import Elasticsearch
from passlib.hash import sha256_crypt
import getpass

# Log entry regex pattern
log_pattern = r"(\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(.*)"

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
es_index = "logs"

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

def fetch_log_entries_from_elasticsearch():
    """
    Fetch log entries from Elasticsearch.
    """
    response = es.search(index=es_index, body={"query": {"match_all": {}}}, size=2000)
    hits = response["hits"]["hits"]
    return hits

def fetch_log_entries_from_postgresql():
    """
    Fetch log entries from PostgreSQL.
    """
    cursor = conn.cursor()
    select_query = "SELECT * FROM logs"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    return rows

# Fetch log entries from Elasticsearch
es_log_entries = fetch_log_entries_from_elasticsearch()

# Fetch log entries from PostgreSQL
pg_log_entries = fetch_log_entries_from_postgresql()

# Print log entries from Elasticsearch
print("Log Entries in Elasticsearch:")
for hit in es_log_entries:
    timestamp = hit["_source"]["timestamp"]
    log_level = hit["_source"]["log_level"]
    log_message = hit["_source"]["log_message"]
    print(f"Timestamp: {timestamp}, Log Level: {log_level}, Log Message: {log_message}")

# Print log entries from PostgreSQL
print("Log Entries in PostgreSQL:")
for row in pg_log_entries:
    timestamp = row[1]
    log_level = row[2]
    log_message = row[3]
    print(f"Timestamp: {timestamp}, Log Level: {log_level}, Log Message: {log_message}")

# Close the PostgreSQL connection
conn.close()