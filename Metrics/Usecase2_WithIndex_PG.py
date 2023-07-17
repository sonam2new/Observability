import pandas as pd
import psycopg2
from passlib.hash import sha256_crypt
import getpass
import time

username = input("Enter username")
password = getpass.getpass("Enter password")

#Initialize PostgreSQL connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

cursor = conn.cursor()

# Define the query parameters
sensor_name = "PER_AIRMON_MESH1903150"

#Measure execution time
start_time = time.time()

# Execute the query to fetch data for the specific sensor name
query = "SELECT timestamp, value FROM urban_sensors WHERE sensor_name = %s"

cursor.execute(query, (sensor_name,))
results = cursor.fetchall()

row_count = cursor.rowcount

# Close the connections
cursor.close()
conn.close()

execution_time = time.time() - start_time

# Save results to a spreadsheet (same as before)
output_file = "Metric_Output.xlsx"
metadata_df = pd.DataFrame({"Query": [query], "Execution time (seconds)": [execution_time]})

metadata = pd.read_excel(output_file, sheet_name="Metadata")
metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)
with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

columns = ['Timestamp', 'Value']
df = pd.DataFrame(results, columns=columns)

print("\nResults:")
print(df.to_string(index=True, justify='left'))

print("\nNumber of data: {}".format(row_count))
print("Execution Time: {:.2f} seconds".format(execution_time))
