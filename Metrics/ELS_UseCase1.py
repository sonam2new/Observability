import pandas as pd
from elasticsearch import Elasticsearch
import time

# Connect to Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = "observatory_data_v2"

# Define the query parameters
sensor_name = "PER_ENVIRONMENT_008632_EA_TPRG"
start_timestamp = "2021-01-01 00:00:00"
end_timestamp = "2022-09-01 00:59:00"


# Elasticsearch Query
es_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"Sensor Name": sensor_name}},
                {"range":{"Timestamp":{"gte":start_timestamp, "lte":end_timestamp}}}
            ]
        }
    }, "size":10000
}

# Perform the search request
response = es.search(index=index_name, body=es_query, scroll="1m")

scroll_id = response["_scroll_id"]
hits = response["hits"]["hits"]

data = []
for hit in hits:
    timestamp = hit["_source"]["Timestamp"]
    value = hit["_source"]["Value"]
    data.append((timestamp, value))

while hits:
    response = es.scroll(scroll_id=scroll_id, scroll="1m")
    hits = response["hits"]["hits"]

    for hit in hits:
        timestamp = hit["_source"]["Timestamp"]
        value = hit["_source"]["Value"]
        data.append((timestamp, value))

    scroll_id = response["_scroll_id"]

# Measure execution time for Elasticsearch
start_time = time.time()
response = es.search(index=index_name, body=es_query)
execution_time_es = time.time() - start_time

metadata_df = pd.DataFrame({"Query": [es_query], "Execution time (seconds)": [execution_time_es]})

# Save results to a spreadsheet
output_file = "Metric_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Print the results
print("Elasticsearch Execution Time: {:.2f} seconds".format(execution_time_es))

#convert Elasticsearch results into Dataframe
es_data = []
for hit in data:
    source = hit
    es_data.append(source)

es_df = pd.DataFrame(es_data, columns=["Timestamp", "Value"])

#Print dataframes
print("\nElasticsearch Results:\n", es_df)
