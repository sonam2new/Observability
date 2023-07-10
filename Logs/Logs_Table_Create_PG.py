import glob
import re
import json
import time
import psycopg2
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

# Define the table name and columns
table_name = "parsed_logs"
"""
columns = {
    "id": "SERIAL PRIMARY KEY",
    "timestamp": "TIMESTAMP",
    "severity": "VARCHAR(20",
    "log_source": "VARCHAR(100)",
    "message": "TEXT",
    "error": "BOOLEAN",
    "exception": "BOOLEAN"
}

"""
# Create the table if it doesn't exist
query = f"ALTER TABLE {table_name} ADD COLUMN file_name VARCHAR(100)"

cursor.execute(query)

print("Table created")