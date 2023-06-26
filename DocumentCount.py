from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name
index_name = "observatory_data_v2"

mapping = es.indices.get_mapping(index=index_name)

fields = mapping[index_name]['mappings']['properties']
for field, info in fields.items():
    ftype = info["type"]
    print(f'Field:{field}, Type:{ftype}')
# Get the count of documents in the index
response = es.count(index=index_name)
print("Total Documents in Elasticsearch: {}".format(response["count"]))
