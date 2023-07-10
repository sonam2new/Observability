
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime
import time

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"

# Define the sensor name
sensor_name = "PER_EMLFLOOD_UO-WALLSENDFS"

# Define the aggregation query
aggregation_query = {
    "query": {
        "term": {"Sensor Name": sensor_name}
    },
    "aggs": {
        "monthly_stats": {
            "date_histogram": {
                "field": "Timestamp",
                "calendar_interval": "month",
                "format": "yyyy-MM"
            },
            "aggs": {
                "summary_stats": {
                    "stats": {
                        "field": "Value"
                    }
                }
            }
        }
    }
}

# Measure execution time
start_time = time.time()

# Perform the aggregation query
response = es.search(index=index_name, body=aggregation_query, size=0)

# Calculate execution time
execution_time = time.time() - start_time

# Get the aggregation data
data = response["aggregations"]["monthly_stats"]["buckets"]

# Create lists to store the results
months = []
averages = []
minimums = []
maximums = []
counts = []

# Extract the statistical summaries for each month
for bucket in data:
    month = bucket["key_as_string"]
    stats = bucket["summary_stats"]

    months.append(month)
    averages.append(stats["avg"])
    minimums.append(stats["min"])
    maximums.append(stats["max"])
    counts.append(stats["count"])

# Create a DataFrame from the results
data = {
    "Sensor Name": [sensor_name] * len(months),
    "Month": months,
    "Average Value": averages,
    "Minimum Value": minimums,
    "Maximum Value": maximums,
    "Count": counts
}
df = pd.DataFrame(data)

df = df.dropna()

# Print the DataFrame
print(df)

# Print execution time
print("Execution Time: {:.2f} seconds".format(execution_time))
