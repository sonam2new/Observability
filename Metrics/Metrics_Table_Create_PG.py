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
    dbname="observatory_metrics",
    user=username,
    password=sha256_crypt.hash(password)
)
conn.autocommit = True
# Create a cursor object to execute SQL statements
cursor = conn.cursor()

# Define the table name and columns
table_name = "urban_sensors"
columns = {
    "sensor_name": "VARCHAR(255)",
    "variable": "VARCHAR(255)",
    "units": "VARCHAR(255)",
    "timestamp": "TIMESTAMP",
    "value": "FLOAT",
    "location": "TEXT",
    "ground_height": "FLOAT",
    "sensor_height": "FLOAT",
    "broker": "VARCHAR(255)",
    "third_party": "BOOLEAN",
    "longitude": "FLOAT",
    "latitude": "FLOAT",
    "raw_id": "INTEGER"
}


# Create the table if it doesn't exist
query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for column, type in columns.items():
    query += f"{column} {type}, "
query = query.rstrip(", ") + ")"
cursor.execute(query)

print("Table created")