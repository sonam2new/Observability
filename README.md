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

