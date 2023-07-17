import pandas as pd
import psycopg2
import time
import getpass
from passlib.hash import sha256_crypt

username = input("Enter username")
password = getpass.getpass("Enter password")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

# Define the query parameters
sensor_name = "PER_ENVIRONMENT_008632_EA_TPRG"
start_timestamp = "2021-01-01 00:00:00"
end_timestamp = "2022-09-01 00:59:00"

# PostgreSQL Query
pg_query = """
    SELECT timestamp, value
    FROM urban_sensors
    WHERE "sensor_name" = %s
    AND "timestamp" BETWEEN %s AND %s
"""

# Measure execution time for PostgreSQL
cursor = conn.cursor()
start_time = time.time()
cursor.execute(pg_query, (sensor_name, start_timestamp, end_timestamp))
results_pg = cursor.fetchall()
execution_time_pg = time.time() - start_time

# Save results to a spreadsheet (same as before)
output_file = "Metric_Output.xlsx"
metadata_df = pd.DataFrame({"Query": [pg_query], "Execution time (seconds)": [execution_time_pg]})

metadata = pd.read_excel(output_file, sheet_name="Metadata")
metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)
with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)


print("PostgreSQL Execution Time: {:.2f} seconds".format(execution_time_pg))
for result in results_pg:
    timestamp=result[0]
    value=result[1]


#convert Postgresql results into Dataframe
pg_results = [desc[0] for desc in cursor.description]
pg_df = pd.DataFrame(results_pg, columns=pg_results)

#Print dataframes
print("\nPostgresql Results:\n", pg_df)


# Close the connections
cursor.close()
conn.close()
