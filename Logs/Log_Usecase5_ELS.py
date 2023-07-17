
import pandas as pd
from elasticsearch import Elasticsearch

# Elasticsearch connection setup
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = 'parsed_logs'

# Search on keyword field
keyword_query = {
    "size": 10000,
    "query": {
        "term": {
            "log_source": "org.apache.hadoop.hdfs.server.datanode.DataNode"
        }
    },
    "_source": ["timestamp", "severity", "message"]
}

# Measure execution time for keyword search
start_time_keyword = pd.Timestamp.now()
response_keyword = es.search(index=index_name, body=keyword_query)
end_time_keyword = pd.Timestamp.now()
execution_time_keyword = (end_time_keyword - start_time_keyword).total_seconds()

# Search on text field
text_query = {
    "size": 10000,
    "query": {
        "match": {
            "message": "error exception"
        }
    },
    "_source": ["timestamp", "severity", "message"]
}

# Measure execution time for text search
start_time_text = pd.Timestamp.now()
response_text = es.search(index=index_name, body=text_query)
end_time_text = pd.Timestamp.now()
execution_time_text = (end_time_text - start_time_text).total_seconds()

metadata_df = pd.DataFrame({"Query": [text_query], "Execution time (seconds)": [execution_time_text]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Process and print results for keyword search
hits_keyword = response_keyword["hits"]["hits"]
results_keyword = pd.DataFrame([
    {"timestamp": hit["_source"]["timestamp"], "severity": hit["_source"]["severity"], "message": hit["_source"]["message"]}
    for hit in hits_keyword
])


print("Elasticsearch Results (Keyword):")
print(results_keyword)
print("Execution time (Keyword):", execution_time_keyword, "seconds")

# Process and print results for text search
hits_text = response_text["hits"]["hits"]
results_text = pd.DataFrame([
    {"timestamp": hit["_source"]["timestamp"], "severity": hit["_source"]["severity"], "message": hit["_source"]["message"]}
    for hit in hits_text
])

metadata_df = pd.DataFrame({"Query": [keyword_query], "Execution time (seconds)": [execution_time_keyword]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

print("Elasticsearch Results (Text):")
print(results_text)
print("Execution time (Text):", execution_time_text, "seconds")

# Compare the execution times
execution_time_diff = execution_time_keyword - execution_time_text
print("Execution Time Difference: {} seconds".format(execution_time_diff))
