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

# Create an index on the sensor_name column
index_query = "CREATE INDEX IF NOT EXISTS sensor_index ON urban_sensors(sensor_name)"
cursor = conn.cursor()
cursor.execute(index_query)

# Define the query parameters
sensor_name = "PER_EMLFLOOD_UO-WALLSENDFS"

#Measure execution time
start_time = time.time()

# Execute the query to fetch data for the specific sensor name
query = "SELECT timestamp, value FROM urban_sensors WHERE sensor_name = %s"

cursor.execute(query, (sensor_name,))
results = cursor.fetchall()

row_count = cursor.rowcount

# Calculate the size on disk for the result
size_query = "SELECT pg_size_pretty(pg_total_relation_size('sensor_index'))"
cursor.execute(size_query)
size_result = cursor.fetchone()
size_on_disk = size_result[0]

# Close the connections
cursor.close()
conn.close()

execution_time = time.time() - start_time
columns = ['Timestamp', 'Value']
df = pd.DataFrame(results, columns=columns)

print("\nResults:")
print(df.to_string(index=True, justify='left'))

print("\nNumber of data: {}".format(row_count))
print("Size on Disk: {}".format(size_on_disk))
print("Execution Time: {:.2f} seconds".format(execution_time))
