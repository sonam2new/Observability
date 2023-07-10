import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime

# Elasticsearch connection setup
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = 'parsed_logs'

start_time = "2015-08-01 15:00:00,000"
end_time = "2017-08-11 20:00:00,800"

start_time_ms = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S,%f")
end_time_ms= datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S,%f")

# Elasticsearch query
es_query = {
    "size": 0,
    "query": {
        "range": {
            "timestamp": {
                "gte": start_time,
                "lte": end_time
            }
        }
    },
    "aggs": {
        "top_log_sources": {
            "terms": {
                "field": "log_source",
                "size": 5,
                "order": {
                    "_count": "desc"
                }
            }
        }
    }
}

# Measure execution time
start_time = pd.Timestamp.now()
response = es.search(index=index_name, body=es_query)
end_time = pd.Timestamp.now()
execution_time_es = (end_time - start_time).total_seconds()

# Process and print results
buckets = response["aggregations"]["top_log_sources"]["buckets"]
results_es = pd.DataFrame([
    {"log_source": bucket["key"], "log_entries_count": bucket["doc_count"]}
    for bucket in buckets
])

print("Elasticsearch Results:")
print(results_es)
print("Execution time (Elasticsearch):", execution_time_es, "seconds")

