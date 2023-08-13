import psycopg2
from passlib.hash import sha256_crypt
import getpass
import time
import tabulate

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
def run_query(sensor_name):
    query = """
    SELECT sensor_name, timestamp, value
    FROM urban_sensors
    WHERE sensor_name= %s
    """

    try:
        cursor = conn.cursor()
        cursor.execute(query, (sensor_name,))
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print("Query execution failed:", e)
        return []

# Run the query and measure the execution time
start_time = time.time()
sensor_name = "PER_NE_CAJT_STA194_LL2_TD1"
results = run_query(sensor_name)
end_time = time.time()
execution_time_postgres = end_time - start_time

table_headers = ["Sensor","Timestamp", "Value"]
formatted_result = tabulate.tabulate(results, headers=table_headers)
print("Results:\n", formatted_result)
print("Execution Time:", execution_time_postgres)
