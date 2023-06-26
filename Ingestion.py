from elasticsearch import Elasticsearch
import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define index name
index_name = "observatory_data"

# Define the Elasticsearch query
query = {
    "size": 0,
    "aggs": {
        "sensors": {
            "terms": {
                "field": "Sensor Name.keyword",
                "size": 10
            },
            "aggs": {
                "timestamps": {
                    "terms": {
                        "field": "Timestamp.keyword",
                        "size": 10
                    },
                    "aggs": {
                        "values": {
                            "terms": {
                                "field": "Value.keyword",
                                "size": 10
                            }
                        }
                    }
                }
            }
        }
    }
}

# Perform the search query
response = es.search(index=index_name, body=query)

# Extract the aggregations and build a list of dictionaries for the results
results = []
sensor_buckets = response["aggregations"]["sensors"]["buckets"]
for sensor_bucket in sensor_buckets:
    sensor_name = sensor_bucket["key"]
    timestamp_buckets = sensor_bucket["timestamps"]["buckets"]
    for timestamp_bucket in timestamp_buckets:
        timestamp = timestamp_bucket["key"]
        value_buckets = timestamp_bucket["values"]["buckets"]
        values = [value_bucket["key"] for value_bucket in value_buckets]
        result = {
            "Sensor Name": sensor_name,
            "Timestamp": timestamp,
            "Values": values
        }
        results.append(result)

# Create a DataFrame from the results
df = pd.DataFrame(results)

# Display the DataFrame
print(df)