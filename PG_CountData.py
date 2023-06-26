
import psycopg2
from passlib.hash import sha256_crypt
import getpass

username = input("Enter username")
password = getpass.getpass("Enter password")

#Initialize PostgreSQL connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)

# Define the sensor name
sensor_name = 'PER_EMLFLOOD_UO-SUNDERLANDFS'

# Create a cursor object
cursor = conn.cursor()

# Define the query to count the documents
query = "SELECT COUNT(*) FROM urban_sensors WHERE sensor_name = %s"

# Execute the query
cursor.execute(query, (sensor_name,))

# Fetch the count of documents
count = cursor.fetchone()[0]

# Print the count
print("Count of documents matching sensor name '{}': {}".format(sensor_name, count))

# Close the cursor and connection
cursor.close()
conn.close()
