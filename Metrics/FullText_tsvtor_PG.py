import pandas as pd
import psycopg2
from passlib.hash import sha256_crypt
import getpass
import time

# Initialize PostgreSQL connection
username = input("Enter username: ")
password = getpass.getpass("Enter password: ")

conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)
conn.autocommit = True
cursor = conn.cursor()

# Add tsvector column
alter_table_query = """
    ALTER TABLE urban_sensors
    ADD COLUMN sensor_name_vector tsvector;
"""
cursor.execute(alter_table_query)

# Update sensor_name_vector column with tsvector values
update_vector_query = """
    UPDATE urban_sensors
    SET sensor_name_vector = to_tsvector('english', sensor_name);
"""
cursor.execute(update_vector_query)

# Create GIN index on sensor_name_vector column
create_index_query = """
    CREATE INDEX index_sensor_name_vector
    ON urban_sensors
    USING gin(sensor_name_vector);
"""
cursor.execute(create_index_query)

# Define the search query using tsvector
sensor_name = "EML:* | FLOOD:*"

# Measure execution time
start_time = time.time()

# Define the tsvector search query
query = """
    SELECT "timestamp", "value"
    FROM urban_sensors
    WHERE sensor_name_vector @@ to_tsquery('english', %s);
"""

# Perform the tsvector search query
cursor.execute(query, (sensor_name, ))
data = cursor.fetchall()

# Calculate the count of timestamps
count = len(data)

# Calculate execution time
execution_time = time.time() - start_time

# Convert data to DataFrame
columns = ["Timestamp", "Value"]
df = pd.DataFrame(data, columns=columns)

# Save results to a spreadsheet (same as before)
output_file = "Metric_Output.xlsx"
metadata_df = pd.DataFrame({"Query": [query], "Execution time (seconds)": [execution_time]})

metadata = pd.read_excel(output_file, sheet_name="Metadata")
metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)
with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Print the results (same as before)
print("Results:")
print(df.to_string(index=False, justify="left"))
print("\nCount of timestamps associated with sensor '{}".format(count))
print("Execution Time: {:.2f} seconds".format(execution_time))