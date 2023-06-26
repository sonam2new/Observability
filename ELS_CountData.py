
from elasticsearch import Elasticsearch

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"
sensor_name = "PER_EMLFLOOD_UO-SUNDERLANDFS"

# Define the query to count the documents
count_query = {
    "query": {
        "match": {"Sensor Name": sensor_name}
    }
}

# Perform the count request
count_response = es.count(index=index_name, body=count_query)
count = count_response["count"]

# Print the count
print("Count of documents matching sensor name '{}': {}".format(sensor_name, count))
