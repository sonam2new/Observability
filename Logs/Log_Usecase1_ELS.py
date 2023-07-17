import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

# Elasticsearch connection setup
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = 'parsed_logs'

start_time = "2016-08-01 15:00:00,000"
end_time = "2016-08-11 20:00:00,200"

start_time_ms = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S,%f")
end_time_ms= datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S,%f")

# Retrieve log lines between a specific timestamp range
query = {
    "query": {
        "range": {
            "timestamp": {
                "gte": start_time,
                "lte": end_time,
            }
        }
    },"size": 10000
}

# Measure execution time
start = datetime.now()

response = es.search(index=index_name, body=query, scroll="2m")
scroll_id = response["_scroll_id"]
hits = response.get("hits", {}).get("hits",[])
results = [hit.get("_source") for hit in hits]

while hits:
    response = es.scroll(scroll_id=scroll_id, scroll="2m")
    scroll_id = response["_scroll_id"]
    hits = response.get("hits", {}).get("hits",[])
    results.extend([hit.get("_source") for hit in hits])

logs_df = pd.DataFrame(results)

end = datetime.now()
execution_time = (end - start).total_seconds()

metadata_df = pd.DataFrame({"Query": [query], "Execution time (seconds)": [execution_time]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Print the results as DataFrames
print("Retrieved logs:")
print(logs_df)
print("\nExecution time:", execution_time, "seconds")