
from elasticsearch import Elasticsearch
from datetime import datetime

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = 'parsed_logs'
start_time = "2016-08-01 15:00:00,000"
end_time = "2016-08-11 20:00:00,200"

start_time_ms = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S,%f")
end_time_ms= datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S,%f")
# Perform a search query to initialize the scroll
query = {
    "query": {
        "range": {
            "timestamp": {
                "gte": start_time,
                "lte": end_time,
            }
        }
    }
}

scroll_time = '1m'  # Adjust the scroll time as needed
result = es.search(index=index_name, body=query, scroll=scroll_time)

scroll_id = result['_scroll_id']
hits = result['hits']['hits']

# Retrieve and display log documents
while len(hits) > 0:
    for hit in hits:
        log = hit['_source']
        #print(log)

    result = es.scroll(scroll_id=scroll_id, scroll=scroll_time)
    scroll_id = result['_scroll_id']
    hits = result['hits']['hits']

print(f"Total log lines stored in Elasticsearch: {result['hits']['total']['value']}")


