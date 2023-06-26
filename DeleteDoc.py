from elasticsearch import Elasticsearch

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "hdfs_log"

response = es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})

if response['deleted']:

    print("Logs deleted")

else:
    print("Failed")


