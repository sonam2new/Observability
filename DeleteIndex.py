import pandas as pd
import psycopg2
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

# Delete index
index_name = "index_log_source"
query = f"DROP INDEX {index_name};"
cursor.execute(query)

print("Index deleted successfully")

# Close the cursor and connection
cursor.close()