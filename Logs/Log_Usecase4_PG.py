
import pandas as pd
import psycopg2
from datetime import datetime
import getpass
from passlib.hash import sha256_crypt

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

start_time = "2015-08-01 15:00:00,000"
end_time = "2017-08-11 20:00:00,800"

start_time_t = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S,%f").strftime("%Y-%m-%d %H:%M:%S")
end_time_t = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S,%f").strftime("%Y-%m-%d %H:%M:%S")

# PostgreSQL query
pg_query = """
    SELECT log_source, COUNT(*) AS log_entries_count
    FROM parsed_logs
    WHERE timestamp BETWEEN %s AND %s
    GROUP BY log_source
    ORDER BY log_entries_count DESC
    LIMIT 5;
"""

# Measure execution time
start_time = pd.Timestamp.now()
cursor.execute(pg_query, (start_time_t, end_time_t))
results_pg = cursor.fetchall()
end_time = pd.Timestamp.now()
execution_time_pg = (end_time - start_time).total_seconds()

# Process and print results
results_pg = pd.DataFrame(results_pg, columns=["log_source", "log_entries_count"])

print("PostgreSQL Results:")
print(results_pg)
print("Execution time (PostgreSQL):", execution_time_pg, "seconds")
