# Observability Database Evaluation
## Introduction
This project aims to evaluate different open-source database options to assess their suitability for storing all signal tyoes.

## Features

- Data collection from urban observatory.
- Data storage using Elasticsearch and PostgreSQL.
- Data analysis.
- Integration with Python.
- Execution Time comparison between Elasticsearch and PostgreSQL queries

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
    - Database 'observatory_metrics' is already created - *psql -d observatory_metrics -U user*

### Steps

1. Clone the repository to your local machine using following command: *git clone https://github.com/sonam2new/Observability.git*
2. Install the required dependencies: *pip install -r requirements.txt*
3. Make sure both Elasticsearch and PostgreSQL server are up and running.

## Data

The metrics data used in this project was obtained from https://urbanobservatory.ac.uk/.

## Experiments

Several experiments were conducted on the metrics data to explore different aspects of performance. Please see experiments in detail and their objective:

### Experiment 1: Compare the execution time between Elasticsearch and PostgreSQL

Run a query in both Elasticsearch and PostgreSQL with a particular sensor name, range of timestamp, and values, ordered by timestamp, to help Red Hat decide which database is suitable and taking less execution time to fetch the expected result.

### Experiment 2: Indexing on PostgreSQL

Create an index on the PostgreSQL database and run a query to fetch all the associated timestamp and value for a particular sensor name using index.
Measure the execution time and find size on disk. 

### Experiment 3: Indexing on Elasticsearch

Run a query to fetch all the associated timestamp and value for a particular sensor name using index. Measure the execution time and find size on disk.
Red Hat can advise their customers on leveraging Elasticsearch's indexing capabilities.

### Experiment 4: Query Performance without Indexing

Perform queries without creating an index and compare the execution time with and without indexing. Measure the size on disk.
Red Hat can highlight the impact of indexing on query performance.

### Experiment 6: Aggregation Queries

Perform aggregation queries on both PostgreSQL and Elasticsearch to compute statistical summaries (e.g., average, minimum, maximum, count of Value) for a specific sensor.

### Experiment 7: Full Text Search in Elasticsearch

Perform a full-text search query in Elasticsearch to evaluate search accuracy. Red Hat can demonstrate the search capabilities of Elasticsearch.

### Experiment 8: Time-Based Queries on Elasticsearch

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Handle time-based queries efficiently for real-time monitoring, historical analysis, and trend identification. Measure execution time and disk space.
Red Hat, as a provider of Elasticsearch, can showcase the benefits of Elasticsearch, including time-based partitioning.

### Experiment 9: Time-Based Queries on PostgreSQL

Run a query to calculate average Value monthly for a particular sensor name in a given timestamp range. Measure execution time and disk space.

## Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.
