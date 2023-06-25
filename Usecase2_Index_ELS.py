import pandas as pd
from elasticsearch import Elasticsearch
import time
# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"

sensor_name ="PER_EMLFLOOD_UO-WALLSENDFS"

start_time = time.time()

# Query
query = {
    "query": {
        "term": {"Sensor Name": sensor_name}
    }, "size": 1000,
    "sort": [{"Timestamp": "asc"}]
}

response = es.search(index=index_name, body=query, scroll="2m")
scroll_id = response["_scroll_id"]

data = []
while True:
    response = es.scroll(scroll_id=scroll_id, scroll="2m")
    hits = response["hits"]["hits"]

    if not hits:
        break;

    for hit in hits:
        timestamp = hit["_source"]["Timestamp"]
        value = hit["_source"]["Value"]
        data.append((timestamp, value))

    scroll_id = response["_scroll_id"]

execution_time = time.time() - start_time

columns = ["Timestamp", "Value"]
es_df = pd.DataFrame(data, columns=columns)

# Calculate size on disk
size_disk = es.cat.indices(index=index_name, format="json", bytes="b")[0]["store.size"]
size_mb = int(size_disk) / (1024 * 1024)

print("\nResults:")
print(es_df.to_string(index=False, justify="left"))

print("\nSize on Disk: {}".format(size_mb))
print("Execution Time: {:.2f} seconds".format(execution_time))
print("\nNumber of data returned: {}".format(len(data)))
