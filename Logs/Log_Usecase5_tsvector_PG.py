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


# Add tsvector column
alter_table_query = """
    ALTER TABLE parsed_logs
    ADD COLUMN message_vector tsvector;
"""
cursor.execute(alter_table_query)

# Update message_vector column
update_vector_query = """
    UPDATE parsed_logs
    SET message_vector = to_tsvector('english', message);
"""
cursor.execute(update_vector_query)

# Create GIN index
create_index_query = """
    CREATE INDEX index_message_vector
    ON parsed_logs
    USING gin(message_vector);
"""
cursor.execute(create_index_query)


# PostgreSQL query
pg_query = """
    SELECT timestamp, severity, message, file_name
    FROM parsed_logs
    WHERE message_vector @@ to_tsquery('english', 'error | exception');
"""

# Measure execution time
start_time = pd.Timestamp.now()
cursor.execute(pg_query)
results_pg = cursor.fetchall()
end_time = pd.Timestamp.now()
execution_time_pg = (end_time - start_time).total_seconds()
"""
metadata_df = pd.DataFrame({"Query": [pg_query], "Execution time (seconds)": [execution_time_pg]})

# Save results to a spreadsheet
output_file = "Log_Output.xlsx"

with pd.ExcelWriter(output_file) as writer:
    metadata_df.to_excel(writer, sheet_name="Metadata", index=False)
"""
# Process and print results
results_pg = pd.DataFrame(results_pg, columns=["timestamp", "severity", "message", "file_name"])
results_pg['file_name'] = results_pg['file_name'].apply(lambda x: os.path.basename(x))


print("PostgreSQL Results:")
print(results_pg)
print("Execution time (PostgreSQL):", execution_time_pg, "seconds")
