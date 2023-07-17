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

# Define the search query using ILIKE
sensor_text = "%EML%FLOOD%UO%CHOP%"

# Measure execution time
start_time = time.time()

# Define the ILIKE search query
query = """
    SELECT "timestamp", "value"
    FROM urban_sensors
    WHERE "sensor_name" ILIKE %s;
"""

# Perform the ILIKE search query
cursor.execute(query, (sensor_text, ))
data = cursor.fetchall()

# Calculate the count of timestamps
count = len(data)

# Calculate execution time
execution_time = time.time() - start_time

# Convert data to DataFrame
columns = ["timestamp", "value"]
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
print("\nCount of timestamps associated with sensor '{}': {}".format(sensor_text, count))
print("Execution Time: {:.2f} seconds".format(execution_time))

