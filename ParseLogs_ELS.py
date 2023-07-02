import glob
import re
import json
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
batch_size = 10000
indexed_count = 0

# Define explicit mapping for log fields
mapping = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss, SSS"},
            "level": {"type": "keyword"},
            "log_source": {"type": "keyword"},
            "message": {"type": "text"},
            "error": {"type": "boolean"}
        }
    }
}

# Create the index with explicit mapping
index_name = "parsed_logs"
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)

# Get the list of log files
log_files = glob.glob('/home/sonamsingh/Downloads/HDFS_2/*.log')
start_time = time.time()

try:
    response = es.count(index=index_name)
    indexed_count = response['count']
except Exception as e:
    print("Error:", e)

if indexed_count > 0:
    log_files = log_files[indexed_count // batch_size + 1:]

# Function to process a single log file and extract log entries
def process_log_file(log_file):
    log_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+) (\S+): (.+)$'
    parsed_logs = []

    with open(log_file, 'r') as file:
        for line in file:
            match = re.match(log_pattern, line)
            if match:
                timestamp = match.group(1)
                level = match.group(2)
                log_source = match.group(3)
                message = match.group(4)

                log_entry = {
                    'timestamp': timestamp,
                    'level': level,
                    'log_source': log_source,
                    'message': message,
                    'error': False
                }

                if level == 'ERROR' or 'exception' in message.lower():
                    log_entry['error'] = True

                parsed_logs.append(log_entry)

    return parsed_logs

# Process and ingest each log file
for log_file in log_files:
    parsed_logs = process_log_file(log_file)

    # Iterate over parsed logs and prepare batches for indexing
    batch = []
    for log in parsed_logs:
        batch.append(log)

        if len(batch) >= batch_size:
            try:
                response = bulk(es, batch, index=index_name)
                indexed_count += len(batch)
                print("Batch ingested. Total documents indexed: {}".format(indexed_count))
            except Exception as e:
                print("Bulk indexing failed:", e)

            batch = []

    if batch:
        try:
            response = bulk(es, batch, index=index_name)
            indexed_count += len(batch)
            print("Batch ingested. Total documents indexed: {}".format(indexed_count))
        except Exception as e:
            print("Bulk indexing failed:", e)

end_time = time.time()
total_time = end_time - start_time
print("Total ingestion time: {} seconds".format(total_time))
print("Indexed {} documents".format(indexed_count))