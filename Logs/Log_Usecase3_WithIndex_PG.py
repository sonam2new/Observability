import pandas as pd
import psycopg2
import getpass
from passlib.hash import sha256_crypt
from datetime import datetime

username = input("Enter username")
password = getpass.getpass("Enter password")

# PostgreSQL connection setup
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="logs_db",
    user=username,
    password=sha256_crypt.hash(password)
)
conn.autocommit = True
cursor = conn.cursor()

# PostgreSQL query
pg_query = """
    SELECT DATE_TRUNC('day', timestamp) AS log_date, COUNT(*) AS log_line_count
    FROM parsed_logs
    WHERE severity = 'ERROR'
    GROUP BY log_date
    ORDER BY log_date;
"""

# Measure execution time
start_time = datetime.now()
cursor.execute(pg_query)
results_pg = cursor.fetchall()
end_time = datetime.now()
execution_time_pg = (end_time - start_time).total_seconds()

metadata_df = pd.DataFrame({"Query": [pg_query], "Execution time (seconds)": [execution_time_pg]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"
metadata = pd.read_excel(output_file, sheet_name="Metadata")

metadata_df = pd.concat([metadata, metadata_df], ignore_index=True)

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

# Process and print results
results_pg = pd.DataFrame(results_pg, columns=["log_date", "log_line_count"])


print("PostgreSQL Results:")
print(results_pg)
print("Execution time (PostgreSQL):", execution_time_pg, "seconds")
