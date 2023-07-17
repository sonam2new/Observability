import pandas as pd
from elasticsearch import Elasticsearch
import time
# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"

sensor_name ="PER_AIRMON_MESH1903150"

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

# Save results to a spreadsheet (same as before)
output_file = "Metric_Output.xlsx"
metadata_df = pd.DataFrame({"Query": [query], "Execution time (seconds)": [execution_time]})

metadata = pd.read_excel(output_file, sheet_name="Metadata")
metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)
with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

columns = ["Timestamp", "Value"]
es_df = pd.DataFrame(data, columns=columns)


print("\nResults:")
print(es_df.to_string(index=False, justify="left"))

print("Execution Time: {:.2f} seconds".format(execution_time))
print("\nNumber of data returned: {}".format(len(data)))
