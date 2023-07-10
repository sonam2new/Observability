import pandas as pd
import psycopg2
from datetime import datetime
import getpass
from passlib.hash import sha256_crypt
import os

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

table_name = "parsed_logs"

# Retrieve log lines between a specific timestamp range
start_time = "2016-08-01 15:00:00,000"
end_time = "2016-08-11 20:00:00,200"

start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S,%f").strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S,%f").strftime("%Y-%m-%d %H:%M:%S")

query = """
    SELECT /*+ INDEX(parsed_logs index_timestamp) */*
    FROM parsed_logs
    WHERE timestamp BETWEEN %s AND %s
    """

# Print the execution plan
explain_query = f"EXPLAIN (FORMAT JSON) {query}"
cursor.execute(explain_query, (start_time, end_time))
plan_result = cursor.fetchone()
execution_plan = plan_result[0][0]

# Measure execution time
start = datetime.now()
cursor.execute(query, (start_time, end_time))
results = cursor.fetchall()
end = datetime.now()
execution_time = (end - start).total_seconds()

# Get column names
column_name = [desc[0] for desc in cursor.description]

logs_df = pd.DataFrame(results, columns=column_name)
logs_df['file_name'] = logs_df['file_name'].apply(lambda x: os.path.basename(x))
print("Retrieved logs:")
print(logs_df)

#print(execution_plan)
# Print the results as DataFrames
print("\nExecution time:", execution_time, "seconds")
