import psycopg2
from elasticsearch import Elasticsearch
import time
import getpass
from passlib.hash import sha256_crypt


# Connect to Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
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

# Define the index name
index_name = "observatory_data_v2"

# Sensor name to query
sensor_name = "PER_WATER_OUSEBURN_SOURCE"

# Elasticsearch query to fetch timestamps and values
query = {
    "query": {
        "term": {
            "Sensor Name": sensor_name
        }
    },
    "sort": [
        {"Timestamp": {"order": "asc"}}
    ],
    "_source": ["Timestamp", "Value"]
}

# Run the query and measure execution time
start_time = time.time()
response = es.search(index=index_name, body=query)
end_time = time.time()
execution_time_elasticsearch = end_time - start_time

# Process and print the results
results = response["hits"]["hits"]
print("Elasticsearch Results:")
for result in results:
    timestamp = result["_source"]["Timestamp"]
    value = result["_source"]["Value"]
    print("Timestamp: {}, Value: {}".format(timestamp, value))

print("Elasticsearch Execution Time: {} seconds".format(execution_time_elasticsearch))
"""
# PostgreSQL query to fetch timestamps and values
query = """
    #SELECT timestamp, value
    #FROM urban_sensors
    #WHERE sensor_name = %s
    #ORDER BY timestamp ASC
"""

# Create a cursor and execute the query
cur = conn.cursor()

# Run the query and measure execution time
start_time = time.time()
cur.execute(query, (sensor_name,))
results = cur.fetchall()
end_time = time.time()
execution_time_postgresql = end_time - start_time

# Close the cursor and the connection
cur.close()
conn.close()

# Process and print the results
print("PostgreSQL Results:")
for result in results:
    timestamp = result[0]
    value = result[1]
    print("Timestamp: {}, Value: {}".format(timestamp, value))

print("PostgreSQL Execution Time: {} seconds".format(execution_time_postgresql))

"""