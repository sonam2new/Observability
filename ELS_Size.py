from elasticsearch import Elasticsearch

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index name for which you want to find the disk size
index_name = "observatory_data_v2"

# Get index statistics
index_stats = es.indices.stats(index=index_name)

# Extract the size of the index on disk in bytes
index_size_bytes = index_stats["_all"]["total"]["store"]["size_in_bytes"]

# Convert the size to human-readable format (e.g., MB, GB, etc.)
def convert_bytes_to_human_readable(size_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

index_size_human_readable = convert_bytes_to_human_readable(index_size_bytes)

# Print the results
print(f"Index size on disk for '{index_name}': {index_size_human_readable}")