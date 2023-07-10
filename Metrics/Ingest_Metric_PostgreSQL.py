import csv
import getpass
import time
import psycopg2
import glob
from passlib.hash import sha256_crypt

username = input("Enter username")
password = getpass.getpass("Enter password")

batch_size = 100000

#Initialize PostgreSQL connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='observatory_metrics',
    user=username,
    password=sha256_crypt.hash(password)
)
conn.autocommit = True
cur = conn.cursor()

total_inserted = 0
batch_count = 0
values = []

#Get the list of csv files
csv_files = glob.glob('/home/sonamsingh/Downloads/Sample/*.csv')

start_time = time.time()
row_ingested = 0
#Iterate over each csv file
for file in csv_files:
    file_name = file.split('/')[-1]
    with open(file, 'r') as csv_files:
        reader = csv.DictReader(csv_files)

        for row in reader:
            try:
                sensor_name = row.get('Sensor Name')
                variable = row.get('Variable')
                units = row.get('Units')
                timestamp = row.get('Timestamp')
                value = row.get('Value')
                location = row.get('Location (WKT)')
                ground_height = row.get('Ground Height Above Sea Level')
                sensor_height = float(row.get('Sensor Height Above Ground'))
                broker = row.get('Broker Name')
                third_party = row.get('Third Party')
                longitude = row.get('Sensor Centroid Longitude')
                latitude = row.get('Sensor Centroid Latitude')
                raw_Id = row.get('Raw ID')

                if any(value == '' for value in [sensor_name, variable, units, timestamp, value, location, ground_height, sensor_height, broker, third_party, longitude, latitude, raw_Id]):
                    continue

                try:
                    ground_height = float(ground_height)
                except (ValueError, TypeError):
                    continue


                values.append((sensor_name, variable, units, timestamp, value, location, ground_height, sensor_height, broker, third_party, longitude, latitude, raw_Id))

                if len(values) == batch_size:
                    args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in values)
                    query = "INSERT INTO urban_sensors (sensor_name, variable, units, timestamp, value, location, ground_height, sensor_height, broker, third_party, longitude, latitude, raw_Id) VALUES" + args_str
                    cur.execute(query)
                    total_inserted +=len(values)
                    batch_count +=1
                    row_ingested += len(values)
                    values = []

                    print(f"Ingested {total_inserted} records in {batch_count} batches")
            except (ValueError, TypeError) as e:
                continue

        elapse_time = time.time() - start_time
        if elapse_time >= 1:
            print(f"Rows ingested per second: {row_ingested}")
            row_ingested = 0
            start_time = time.time()
if values:
    args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in values)
    query = "INSERT INTO urban_sensors (sensor_name, variable, units, timestamp, value, location, ground_height, sensor_height, broker, third_party, longitude, latitude, raw_Id) VALUES" + args_str
    cur.execute(query)

    total_inserted += len(values)
    batch_count +=1
    row_ingested += len(values)


end_time = time.time()
ingestion_time = end_time - start_time

cur.close()
conn.close()

print("Total inserted records:", total_inserted)
print("Total batches", batch_count)
print("Ingestion time:", ingestion_time, "seconds")