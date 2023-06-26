import csv
import time
import glob
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
updated_mapping = {
    "properties": {
        "Sensor Name": {"type": "keyword"},
        "Variable": {"type": "keyword"},
        "Units": {"type": "keyword"},
        "Timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
        "Value": {"type": "float"},
        "Location (WKT)": {"type": "text"},
        "Ground Height Above Sea Level": {"type": "float"},
        "Sensor Height Above Ground": {"type": "float"},
        "Broker Name": {"type": "keyword"},
        "Third Party": {"type": "keyword"},
        "Sensor Centroid Longitude": {"type": "float"},
        "Sensor Centroid Latitude": {"type": "float"},
        "Raw ID": {"type": "keyword"}
    }
}

max_retries = 5
retry_delay = 8  # Delay in seconds between retries
index_name = "observatory_data"
new_index_name = "observatory_data_v2"
es.indices.create(index=new_index_name, body={"mappings": updated_mapping})
body = {
    "source": {"index": index_name},
    "dest": {"index": new_index_name}
}
for attempt in range(max_retries):
    try:

        # Attempt to delete the index
        response = es.reindex(body=body, refresh=True)
        break  # Break the loop if successful
    except (ConnectionError, ConnectionTimeout):
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            # Handle the failure or raise an exception
            print("Failed to delete index after multiple retries.")

# Step 4: Verify the new index and mappings
new_index_mapping = es.indices.get_mapping(index=new_index_name)
print("New index mapping:", new_index_mapping)
"""
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout

max_retries = 5
retry_delay = 8  # Delay in seconds between retries
index_name = "observatory_data"
new_index_name = "observatory_data_v2"

for attempt in range(max_retries):
    try:

        # Attempt to delete the index
        es.indices.delete(index=new_index_name, ignore=[400, 404])
        print("Deletion done")
        break  # Break the loop if successful
    except (ConnectionError, ConnectionTimeout):
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            # Handle the failure or raise an exception
            print("Failed to delete index after multiple retries.")
"""