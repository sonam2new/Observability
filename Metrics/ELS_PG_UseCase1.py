import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
import datetime
import time
import getpass
from passlib.hash import sha256_crypt

# Connect to Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = "observatory_data_v2"

username = input("Enter username")
password = getpass.getpass("Enter password")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

# Define the query parameters
sensor_name = "PER_EMLFLOOD_UO-SUNDERLANDFS"
start_timestamp = "2021-01-01 00:00:00"
end_timestamp = "2022-01-01 00:59:00"

output_file = "../result.txt"

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

# PostgreSQL Query
pg_query = """
    SELECT timestamp, value
    FROM urban_sensors
    WHERE "sensor_name" = %s
    AND "timestamp" BETWEEN %s AND %s
"""

# Measure execution time for Elasticsearch
start_time = time.time()
response = es.search(index=index_name, body=es_query)
execution_time_es = time.time() - start_time

# Measure execution time for PostgreSQL
cursor = conn.cursor()
start_time = time.time()
cursor.execute(pg_query, (sensor_name, start_timestamp, end_timestamp))
results_pg = cursor.fetchall()
execution_time_pg = time.time() - start_time

# Print the results
print("Elasticsearch Execution Time: {:.2f} seconds".format(execution_time_es))

print("PostgreSQL Execution Time: {:.2f} seconds".format(execution_time_pg))
for result in results_pg:
    timestamp=result[0]
    value=result[1]

#convert Elasticsearch results into Dataframe
es_data = []
for hit in data:
    source = hit
    es_data.append(source)

es_df = pd.DataFrame(es_data, columns=["Timestamp", "Value"])

#convert Postgresql results into Dataframe
pg_results = [desc[0] for desc in cursor.description]
pg_df = pd.DataFrame(results_pg, columns=pg_results)

#Print dataframes
print("\nElasticsearch Results:\n", es_df)
print("\nPostgresql Results:\n", pg_df)

with open(output_file, "a") as file:
    file.write("Elasticsearch Execution Time: {:.2f} seconds\n".format(execution_time_es))

    file.write("PostgreSQL Execution Time: {:.2f} seconds\n".format(execution_time_pg))

    file.write("\nElasticsearch Results:\n")
    file.write(str(es_df) + "\n")

    file.write("\nPostgreSQL Results:\n")
    file.write(str(pg_df) + "\n")

# Close the connections
cursor.close()
conn.close()
