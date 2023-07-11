# Observability Database Evaluation
## Introduction
This project aims to evaluate different open-source database options to assess their suitability for storing all signal tyoes.

## Features

- Metrics data collection from urban observatory.
- System Logs collection from HDFS2.
- Data storage in Elasticsearch and PostgreSQL.
- Data analysis.
- Integration with Python.
- Execution Time comparison between Elasticsearch and PostgreSQL queries
- Log file parsing: Parse log files in various formats and extract relevant fields.
- Log filtering: Filter log entries based on specified criteria such as severity level or log source.
- Log analysis: Perform various analysis tasks on the parsed log data, such as counting log entries, aggregating by severity, or calculating average log line count per day.

## Installation

### Pre-requisites
To set up project locally, follow these steps:

- Python 3.11.3: Make sure Python 3.11.3 installed. Install it from CLI
  - Fedora command: *sudo dnf install python*
- Elasticsearch: Install and configure Elasticsearch on local machine.
  - Elasticsearch requires Java, so the first step is to install it - *sudo dnf install java-11-openjdk-devel*
  - Install Elasticsearch from the official repository - *rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch *
  - Next, create a new file called elasticsearch.repo in the /etc/yum.repos.d/ directory - *sudo nano /etc/yum.repos.d/elasticsearch.repo*
    - Add following lines:
      [elasticsearch-7.x]
      name=Elasticsearch repository for 7.x packages
      baseurl=https://artifacts.elastic.co/packages/7.x/yum
      gpgcheck=1
      gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
      enabled=1
      autorefresh=1
      type=rpm-md
      - Save and close the file, then install Elasticsearch with: *sudo dnf install elasticsearch*
      - Configure Elasticsearch: *sudo nano /etc/elasticsearch/elasticsearch.yml*
        - network.host`: Set it to the IP address of server.
        - http.port`: By default, Elasticsearch runs on port 9200.
        - cluster.name`: Name Elasticsearch cluster. By default, it’s called “elasticsearch”.
  - Enable and Start Elasticsearch - *sudo systemctl enable elasticsearch* and *sudo systemctl start elasticsearch*
- PostgreSQL: Install and set PostgreSQL on local machine - *sudo dnf install postgresql postgresql-server*
    - Initialize PG Cluser - *sudo postgresql-setup --initdb --unit postgresql*
    - Start cluster - *sudo systemctl start postgresql*
    - Login as DB admin - *sudo su - postgres*
  # For Metrics:
    - Run command - *createdb observatory_metrics --owner username*
    - Restart the server - *sudo systemctl restart postgresql*
    - Run command- *psql -d observatory_metrics -U user*
    - Create the table for Metrics by running python script **Metrics_Table_Create_PG.py**
  
  # For Logs:
    - Run command - *createdb logs_db --owner username*
    - Restart the server - *sudo systemctl restart postgresql*
    - Run command- *psql -d parsed_logs -U user*
    - Create the table for Metrics by running python script **Logs_Table_Create_PG.py**

### Steps

1. Clone the repository to your local machine using following command: *git clone https://github.com/sonam2new/Observability.git*
2. Install the required dependencies: *pip install -r requirements.txt*
3. Make sure both Elasticsearch and PostgreSQL server are up and running.

## Data

1. The metrics data used in this project was obtained from https://urbanobservatory.ac.uk/.
2. Raw data of year 2021 and 2022 considered for experiments.
3. Before starting for below Metrics experiments, ingest all the data into both Elasticsearch and PostgreSQL DB using the python script **Ingest_Metric_ELS.py** and **Ingest_Metric_PostgreSQL.py** respectively.
4. System Logs was obtained from https://zenodo.org/record/3227177 (HDFS2).
5. Before starting for below Logs experiments,parsed and ingest logs into both Elasticsearch and PostgreSQL DB using the python script **ParsedLogs_ELS.py** and **ParsedLogs_PG.py** respectively.

## Experiments on Metrics

Several experiments were conducted on the metrics data to explore different aspects of performance. Please see experiments in detail and their objective:

### Experiment 1: Compare the execution time between Elasticsearch and PostgreSQL

Run a query in both Elasticsearch and PostgreSQL with a particular sensor name, range of timestamp, and values, ordered by timestamp, to help Red Hat decide which database is suitable and taking less execution time to fetch the expected result.
To perform this experiment, run python file **ELS_PG_UseCase1.py**.
Output: Results returned by query and Execution time taken by Elasticsearch and PostgreSQL DB.

### Experiment 2: Indexing on PostgreSQL

Create an index on the PostgreSQL database and run a query to fetch all the associated timestamp and value for a particular sensor name using index.
Measure the execution time and find size on disk. 
To perform this experiment, run python file **UseCase2_Index_PG.py**.
Output: Results returned by query and measure execution time taken by PostgreSQL DB. Also, verify size of disk used.

### Experiment 3: Indexing on Elasticsearch

Run a query to fetch all the associated timestamp and value for a particular sensor name using index. Measure the execution time and find size on disk.
Red Hat can advise their customers on leveraging Elasticsearch's indexing capabilities.
To perform this experiment, run python file **UseCase2_Index_ELS.py**.
Output: Results returned by query and measure execution time taken by Elasticsearch DB. Also, verify size of disk used.

### Experiment 4: Query Performance without Indexing

Perform queries without creating an index and compare the execution time with and without indexing. Measure the size on disk.
Red Hat can highlight the impact of indexing on query performance.
To perform this experiment, run python file **UseCase4_WithoutIndex.py**.
Output: Results returned by query and measure execution time taken by PostgreSQL DB. Also, verify size of disk used. compare the execution time with Experiment 2.

### Experiment 5: Aggregation Queries

Perform aggregation queries on both PostgreSQL and Elasticsearch to compute statistical summaries (e.g., average, minimum, maximum, count of Value) for a specific sensor.
To perform this experiment, run python file **Aggregation_ELS.py** and **Aggregation_PG.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

### Experiment 6: Full Text Search in Elasticsearch

Perform a full-text search query in Elasticsearch to evaluate search accuracy. Red Hat can demonstrate the search capabilities of Elasticsearch.
To perform this experiment, run python file **FullText_ELS.py**.
Output: Results returned by query and measure execution time taken by Elasticsearch DB. Also, verify size of disk used.

### Experiment 7: Time-Based Queries on Elasticsearch

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Handle time-based queries efficiently for real-time monitoring, historical analysis, and trend identification. Measure execution time and disk space.
Red Hat, as a provider of Elasticsearch, can showcase the benefits of Elasticsearch, including time-based partitioning.
To perform this experiment, run python file **Timebased.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

### Experiment 8: Time-Based Queries on PostgreSQL

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Measure execution time and disk space.
To perform this experiment, run python file **Timebased.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

## Experiments on Logs

Several experiments were conducted on the Logs to explore different aspects of performance. Please see experiments in detail and their objective:

### Experiment 1: Retrieve Logs for a Particular Timestamp along with File Name/Node from Elasticsearch and PostgreSQL

Run a query in both Elasticsearch and PostgreSQL with a range of timestamp along from which file/node those logs are coming, to help Red Hat in efficiently retrieving logs for a specific timestamp along with the corresponding file name. It allows for quick debugging and troubleshooting.
- Elasticsearch: Run the script **Log_Usecase1_ELS.py** to fetch results from elasticsearch DB.
- PostgreSQL With Index: Firstly create index on timestamp column using script **CreateIndex_PG.py**, then perform query to fetch quicker results using script **Log_Usecase1_WithIndex_PG.py**.
- PostgreSQL Without Index: Drop the index using the script **DeleteIndex.py**, then perform query to fetch result and compare execution time witn index script.
Output: Results returned by query and Execution time taken by Elasticsearch and PostgreSQL DB with & without using indexing.

### Experiment 2: Count the Number of Log Entries for Each Log Source

Create an index on the PostgreSQL database and run a query to fetch all the associated timestamp and value for a particular sensor name using index.
Measure the execution time and find size on disk. 
To perform this experiment, run python file **UseCase2_Index_PG.py**.
Output: Results returned by query and measure execution time taken by PostgreSQL DB. Also, verify size of disk used.

### Experiment 3: Indexing on Elasticsearch

Run a query to fetch all the associated timestamp and value for a particular sensor name using index. Measure the execution time and find size on disk.
Red Hat can advise their customers on leveraging Elasticsearch's indexing capabilities.
To perform this experiment, run python file **UseCase2_Index_ELS.py**.
Output: Results returned by query and measure execution time taken by Elasticsearch DB. Also, verify size of disk used.

### Experiment 4: Query Performance without Indexing

Perform queries without creating an index and compare the execution time with and without indexing. Measure the size on disk.
Red Hat can highlight the impact of indexing on query performance.
To perform this experiment, run python file **UseCase4_WithoutIndex.py**.
Output: Results returned by query and measure execution time taken by PostgreSQL DB. Also, verify size of disk used. compare the execution time with Experiment 2.

### Experiment 5: Aggregation Queries

Perform aggregation queries on both PostgreSQL and Elasticsearch to compute statistical summaries (e.g., average, minimum, maximum, count of Value) for a specific sensor.
To perform this experiment, run python file **Aggregation_ELS.py** and **Aggregation_PG.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

### Experiment 6: Full Text Search in Elasticsearch

Perform a full-text search query in Elasticsearch to evaluate search accuracy. Red Hat can demonstrate the search capabilities of Elasticsearch.
To perform this experiment, run python file **FullText_ELS.py**.
Output: Results returned by query and measure execution time taken by Elasticsearch DB. Also, verify size of disk used.

### Experiment 7: Time-Based Queries on Elasticsearch

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Handle time-based queries efficiently for real-time monitoring, historical analysis, and trend identification. Measure execution time and disk space.
Red Hat, as a provider of Elasticsearch, can showcase the benefits of Elasticsearch, including time-based partitioning.
To perform this experiment, run python file **Timebased.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

### Experiment 8: Time-Based Queries on PostgreSQL

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Measure execution time and disk space.
To perform this experiment, run python file **Timebased.py**.
Output: Results returned by query and measure & compare the execution time taken by Elasticsearch and PostgreSQL DB.

## Results
This project has been extensively tested on a range of log files and has shown excellent performance and accuracy in parsing log data. The extracted log fields provide valuable insights into the log entries and enable efficient log analysis and troubleshooting.

## Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.
