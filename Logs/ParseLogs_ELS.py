import glob
import re
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
            "timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss,SSS"},
            "severity": {"type": "keyword"},
            "log_source": {"type": "keyword"},
            "message": {"type": "text"},
            "error": {"type": "boolean"},
            "startup_message": {"type": "text"},
            "file_name": {"type": "keyword"}  # New field for storing the log file name
        }
    }
}

# Create a new index with explicit mapping
index_name = "parsed_logs"  # Choose a new index name
if es.indices.exists(index=index_name):
    print("Index already exists. Please choose a different index name.")
    exit(1)

es.indices.create(index=index_name, body=mapping)

# Get the list of log files
log_files = glob.glob('/home/sonamsingh/Downloads/HDFS_2/*.log')

start_time = time.time()

# Function to process a single log file and extract log entries
def process_log_file(log_file):
    log_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+) (\S+): (.+)$'
    startup_pattern = r'^STARTUP_MSG: (.+)$'
    parsed_logs = []

    file_name = log_file.split('/')[-1]  # Extract the file name from the file path

    with open(log_file, 'r') as file:
        for line in file:
            match = re.match(log_pattern, line)
            if match:
                timestamp = match.group(1)
                severity = match.group(2)
                log_source = match.group(3)
                message = match.group(4)

                log_entry = {
                    'timestamp': timestamp,
                    'severity': severity,
                    'log_source': log_source,
                    'message': message,
                    'error': False,
                    "startup_message": '',
                    'file_name': file_name  # Assign the log file name to the field
                }

                if severity == 'ERROR' or 'exception' in message.lower():
                    log_entry['error'] = True

                # Check if the log line is a startup_ms
                startup = re.match(startup_pattern, message)
                if startup:
                    startup_msg = startup.group(1)
                    log_entry['startup_msg'] = startup_msg

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
print("Total ingestion time: {} seconds")
print("Index {} documents".format(indexed_count))

# Calculate disk space usage
index_stats = es.indices.stats(index=index_name)
disk_usage_bytes = index_stats['_all']['total']['store']['size_in_bytes']
disk_usage_pretty = index_stats['_all']['total']['store']['size']

print("Disk space usage: {} bytes".format(disk_usage_bytes))
print("Disk space usage: {}".format(disk_usage_pretty))