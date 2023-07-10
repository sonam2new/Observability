import glob
import re
import json
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import getpass
from passlib.hash import sha256_crypt

username = input("Enter username")
password = getpass.getpass("Enter password")

# Initialize PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="logs_db",
    user=username,
    password=sha256_crypt.hash(password)
)
conn.autocommit = True

# Create a cursor object to execute SQL statements
cursor = conn.cursor()
table_name = "parsed_logs"

# Create an index on the log_source column
index_name = "index_timestamp"
column_name = "timestamp"
cursor.execute(f"CREATE INDEX {index_name} ON {table_name} ({column_name})")

# Check if the index is created successfully
cursor.execute(f"SELECT indexname FROM pg_indexes WHERE tablename = '{table_name}' AND indexname = '{index_name}'")
index_exists = cursor.fetchone()

if index_exists:
    print(f"Index {index_name} created successfully on column {column_name}")
else:
    print(f"Failed to create index on column {column_name}")