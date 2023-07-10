from elasticsearch import Elasticsearch

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = "observatory_data_v2"
# Define the index name
sensor_name = "PER_EMLFLOOD_UO-SUNDERLANDFS"
start_timestamp = "2021-01-01 00:00:00"
end_timestamp = "2022-01-01 00:59:00"

query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"Sensor Name": sensor_name}},
                {"range": {"Timestamp": {"gte": start_timestamp, "lte": end_timestamp}}}
            ]
        }
    }, "size": 10000
}

# Search for the documents matching the query
response = es.search(index=index_name, body=query)
hits = response["hits"]["hits"]

# Extract the document IDs to be deleted
doc_ids = [hit["_id"] for hit in hits]

# Delete the documents
for doc_id in doc_ids:
    es.delete(index=index_name, id=doc_id)

print("Deleted {} doc".format(len(doc_ids)))