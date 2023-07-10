import pandas as pd
import psycopg2
from passlib.hash import sha256_crypt
import getpass
import time

# Initialize PostgreSQL connection
username = input("Enter username: ")
password = getpass.getpass("Enter password: ")

conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

# Define the sensor name
sensor_name = "PER_WATER_KINGSTON_PARK"

# Define the aggregation query
query = """
    SELECT
        DATE_TRUNC('month', timestamp) AS month,
        AVG(value) AS average,
        MIN(value) AS minimum,
        MAX(value) AS maximum,
        COUNT(*) AS count
    FROM
        urban_sensors
    WHERE
        sensor_name = %s
    GROUP BY
        month
    ORDER BY
        month
"""

# Measure execution time
start_time = time.time()

# Execute the query
cursor = conn.cursor()
cursor.execute(query, (sensor_name,))
results = cursor.fetchall()

# Calculate execution time
execution_time = time.time() - start_time

# Create a DataFrame from the results
columns = ["Month", "Average Value", "Minimum Value", "Maximum Value", "Count"]
df = pd.DataFrame(results, columns=columns)
df["Sensor Name"] = sensor_name

# Convert the month column to the desired format
df["Month"] = df["Month"].dt.strftime("%Y-%m")

# Print the DataFrame
print(df)

# Print execution time
print("Execution Time: {:.2f} seconds".format(execution_time))

# Close the cursor and connection
cursor.close()
conn.close()