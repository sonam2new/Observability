
import csv
import time
import glob
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Initialize Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
batch_size = 1000000
batch = []
indexed_count = 0

# Define explicit mapping for all columns
mapping = {
    "mappings": {
        "properties": {
            "Sensor Name": {"type": "keyword"},
            "Variable": {"type": "keyword"},
            "Units": {"type": "keyword"},
            "Timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Value": {"type": "float"},
            "Location (WKT)": {"type": "geo_point"},
            "Ground Height Above Sea Level": {"type": "float"},
            "Sensor Height Above Ground": {"type": "float"},
            "Broker Name": {"type": "keyword"},
            "Third Party": {"type": "keyword"},
            "Sensor Centroid Longitude": {"type": "float"},
            "Sensor Centroid Latitude": {"type": "float"},
            "Raw ID": {"type": "keyword"}
        }
    }
}

# Create the index with explicit mapping
index_name = "observatory_data_v2"
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)

# Get the list of CSV files
csv_files = glob.glob('/home/sonamsingh/Downloads/Sample/*.csv')
start_time = time.time()

try:
    response = es.count(index=index_name)
    indexed_count = response['count']
except Exception as e:
    print("Error:", e)

if indexed_count > 0:
    csv_files = csv_files[indexed_count // batch_size + 1:]

# Iterate over each CSV file
for file in csv_files:
    file_indexed_count = 0
    file_name = file.split('/')[-1]
    with open(file, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            if not all(row.values()):
                print("Missing values. Skip row")
                continue

            sensor_name = row.get('Sensor Name')

            # Create the document dictionary
            document = {
                'Sensor Name': row.get('Sensor Name'),
                'Variable': row.get('Variable'),
                'Units': row.get('Units'),
                'Timestamp': row.get('Timestamp'),
                'Value': float(row.get('Value')),
                'Location (WKT)': row.get('Location (WKT)'),
                'Ground Height Above Sea Level': float(row.get('Ground Height Above Sea Level')),
                'Sensor Height Above Ground': float(row.get('Sensor Height Above Ground')),
                'Broker Name': row.get('Broker Name'),
                'Third Party': row.get('Third Party'),
                'Sensor Centroid Longitude': float(row.get('Sensor Centroid Longitude')),
                'Sensor Centroid Latitude': float(row.get('Sensor Centroid Latitude')),
                'Raw ID': row.get('Raw ID')
            }

            batch.append(document)

            if len(batch) >= batch_size:
                try:
                    response = bulk(es, batch, index=index_name)
                    file_indexed_count += len(batch)
                    print("Batch ingested. Total documents indexed: {}".format(indexed_count))
                except Exception as e:
                    print("Bulk indexing failed:", e)

                batch = []

    if batch:
        try:
            response = bulk(es, batch, index=index_name)
            file_indexed_count += len(batch)
            print("Batch ingested. Total documents indexed: {}".format(indexed_count))
        except Exception as e:
            print("Bulk indexing failed:", e)

    indexed_count += file_indexed_count
    print("CSV file '{}' ingested successfully. Total documents indexed: {}".format(file_name, file_indexed_count))

end_time = time.time()
total_time = end_time - start_time
print("Total ingestion time: {} seconds".format(total_time))
print("Indexed {} documents".format(indexed_count))
