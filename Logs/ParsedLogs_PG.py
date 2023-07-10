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

# Define the table name and columns
table_name = "parsed_logs"
columns = {
    "timestamp": "TIMESTAMP",
    "severity": "VARCHAR(20)",
    "log_source": "VARCHAR(100)",
    "message": "TEXT",
    "error": "BOOLEAN",
    "file_name": "VARCHAR(100)"
}

# Add the file_name column to the table if not already present
cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
existing_columns = [row[0] for row in cursor.fetchall()]

if 'file_name' not in existing_columns:
    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN file_name VARCHAR(100)")

# Get the list of log files
log_files = glob.glob('/home/sonamsingh/Downloads/HDFS_2/*.log')
start_time = time.time()

# Define the batch size for batch processing
batch_size = 10000
batch = []

# Define the log pattern for parsing
log_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+) (\S+): (.+)$'
startup_pattern = r'^STARTUP_MSG: (.+)$'

# Define the total count variable
total_count = 0

# Process log files and extract log entries
for log_file in log_files:
    with open(log_file, 'r') as file:
        for line in file:
            match = re.match(log_pattern, line)
            if match:
                timestamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S,%f")
                severity = match.group(2)
                log_source = match.group(3)
                message = match.group(4)

                log_entry = {
                    'timestamp': timestamp,
                    'severity': severity,
                    'log_source': log_source,
                    'message': message,
                    'error': False if severity != 'ERROR' and 'exception' not in message.lower() else True,
                    'file_name': log_file
                }

                batch.append(log_entry)

                # Execute batch insert when batch size is reached
                if len(batch) >= batch_size:
                    execute_values(cursor, f"INSERT INTO {table_name} (timestamp, severity, log_source, message, error, file_name) VALUES %s", [(
                        entry['timestamp'], entry['severity'], entry['log_source'], entry['message'], entry['error'], entry['file_name']) for entry in batch])
                    total_count += len(batch)
                    batch = []

            # Check for STARTUP_MSG lines and insert them separately
            else:
                startup = re.match(startup_pattern, line)
                if startup:
                    startup_msg = startup.group(1)
                    startup_entry = {
                        'timestamp': '1970-01-01 00:00:00.000',
                        'severity': 'STARTUP_MSG',
                        'log_source': '',
                        'message': startup_msg,
                        'error': False,
                        'file_name': log_file
                    }
                    batch.append(startup_entry)

# Insert any remaining entries in the last batch
if batch:
    execute_values(cursor, f"INSERT INTO {table_name} (timestamp, severity, log_source, message, error, file_name) VALUES %s", [(
        entry['timestamp'], entry['severity'], entry['log_source'], entry['message'], entry['error'], entry['file_name']) for entry in batch])
    total_count += len(batch)

end_time = time.time()
total_time = end_time - start_time
cursor.execute("SELECT pg_size_pretty(pg_total_relation_size(%s))", (table_name,))
disk_space = cursor.fetchone()[0]

print("Disk space usage: {}".format(disk_space))
print("Total ingestion time: {} seconds".format(total_time))
print("Indexed {} documents".format(total_count))