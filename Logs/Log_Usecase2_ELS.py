import pandas as pd
from elasticsearch import Elasticsearch

# Elasticsearch connection setup
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = 'parsed_logs'

# Elasticsearch query
es_query = {
    "size": 0,
    "aggs": {
        "log_count_per_source": {
            "terms": {
                "field": "log_source",
                "size": 100,
                "order": {"_count": "desc"}
            }
        }
    }
}

# Measure execution time
start_time = pd.Timestamp.now()
response = es.search(index=index_name, body=es_query)
end_time = pd.Timestamp.now()
execution_time_es = (end_time - start_time).total_seconds()
metadata_df = pd.DataFrame({"Query": [es_query], "Execution time (seconds)": [execution_time_es]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Process and print results
buckets = response["aggregations"]["log_count_per_source"]["buckets"]
results_es = pd.DataFrame([
    {"log_source": bucket["key"], "log_count": bucket["doc_count"]}
    for bucket in buckets
])

print("Elasticsearch Results:")
print(results_es)
print("Execution time (Elasticsearch):", execution_time_es, "seconds")