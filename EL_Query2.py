from elasticsearch import Elasticsearch
import json

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data"
sensor_name ="PER_WATER_KINGSTON_PARK"
# Search for all documents in the index
#query = {
#"query": {
#"match_all": {}
#     }
#}
query = {
    "query": {
        "term": {
            "Sensor Name.keyword": sensor_name
        }
    }
}
result = es.search(index=index_name, body=query, size=8)

timestamps = [hit["_source"]["Timestamp"] for hit in result["hits"]["hits"]]

print("All Timestamp associated with sensor '{}':".format(sensor_name))
for timestamp in timestamps:
    print(timestamp)

