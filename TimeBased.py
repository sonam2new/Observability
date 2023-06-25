import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
from datetime import datetime
import time
from passlib.hash import sha256_crypt
import getpass

username = input("Enter username")
password = getpass.getpass("Enter password")

# Initialize PostgreSQL connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name for Elasticsearch
index_name = "observatory_data_v2"

# Define the sensor name and time range
sensor_name = 'PER_EMLFLOOD_UO-WALLSENDFS'
start_time = "2021-01-01 00:00:00"
end_time = "2022-12-01 00:59:00"

# --- Elasticsearch Query ---

# Create a search query for Elasticsearch
es_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"Sensor Name": sensor_name}},
                {"range": {"Timestamp": {"gte": start_time, "lt": end_time}}}
            ]
        }
    },
    "aggs": {
        "monthly_average": {
            "date_histogram": {
                "field": "Timestamp",
                "calendar_interval": "month",
                "format": "yyyy-MM"
            },
            "aggs": {
                "average_value": {
                    "avg": {
                        "field": "Value"
                    }
                }
            }
        }
    },
    "size": 0
}

# Measure Elasticsearch execution time
es_start_time = time.time()

# Execute the search query in Elasticsearch
es_response = es.search(index=index_name, body=es_query)

# Process the Elasticsearch results
es_hits = es_response["aggregations"]["monthly_average"]["buckets"]
es_data = []
for hit in es_hits:
    key = hit["key_as_string"]
    month_average = hit["average_value"]["value"]
    es_data.append((sensor_name, key, month_average))

# Calculate Elasticsearch execution time
es_execution_time = time.time() - es_start_time

# Create a DataFrame
es_columns = ["Sensor Name", "Month", "Average Value"]
es_df = pd.DataFrame(es_data, columns=es_columns)

df = es_df.dropna()

print("Elastic Result:")
print(df.to_string(index=False))


# Elasticsearch Execution Time and Disk Space
print("\nElasticsearch Execution Time: {:.2f} seconds".format(es_execution_time))

# --- PostgreSQL Query ---

# Create a cursor object for PostgreSQL
pg_cursor = conn.cursor()

pg_start_time = time.time()

# Execute the SQL query to fetch data in PostgreSQL
pg_query = """
    SELECT sensor_name, DATE_TRUNC('month', timestamp) AS month, AVG(value) AS average_value
    FROM urban_sensors
    WHERE sensor_name = %s AND timestamp >= %s AND timestamp < %s
    GROUP BY sensor_name, month
    ORDER BY month
"""
pg_cursor.execute(pg_query, (sensor_name, start_time, end_time))
pg_results = pg_cursor.fetchall()

pg_execution_time = time.time() - pg_start_time


# PostgreSQL results
columns = ['Sensor Name', 'Month', 'Average Value']
df_result = pd.DataFrame(pg_results, columns=columns)

print("PostgreSQL Results:")
print(df_result)


# PostgreSQL Execution Time and Disk Space
print("\nPostgreSQL Execution Time: {:.2f} seconds".format(pg_execution_time))

# Close the cursor and connections
pg_cursor.close()
conn.close()
