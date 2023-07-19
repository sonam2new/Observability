import pandas as pd
from elasticsearch import Elasticsearch
import time
import json

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"

sensor_text = "*EML*FLOOD*"

# Measure execution time
start_time = time.time()

# Define the search query using full-text search
query = {
    "query": {
        "wildcard": {
            "Sensor Name": sensor_text
        }
    },
    "size": 10000
}

# Perform the search request
response = es.search(index=index_name, body=query, scroll="2m")
scroll_id = response["_scroll_id"]

data = []
size = 0
while True:
    # Extract the hits from the response
    hits = response["hits"]["hits"]

    if not hits:
        break

    for hit in hits:
        source = hit["_source"]
        timestamp = source["Timestamp"]
        value = source["Value"]
        data.append((timestamp, value))

        sjson = json.dumps(source)
        size += len(sjson)

    response = es.scroll(scroll_id=scroll_id, scroll="2m")

# Calculate the count of timestamps
count = len(data)
size = sum(len(hit["_source"]) for hit in hits)
# Convert size to human-readable format (e.g., MB)
size_mb = size / (1024 * 1024)

# Calculate execution time
execution_time = time.time() - start_time
"""
metadata_df = pd.DataFrame({"Query": [query], "Execution time (seconds)": [execution_time]})

# Save results to a spreadsheet
output_file = "Metric_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)
"""

# Create a DataFrame from the results
columns = ["Timestamp", "Value"]
df = pd.DataFrame(data, columns=columns)

# Print the results
print("Results:")
print(df.to_string(index=False, justify="left"))


print("\nCount of timestamps associated with sensor '{}': {}".format(sensor_text, count))
print("Execution Time: {:.2f} seconds".format(execution_time))