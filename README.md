# Project Name: Real-Time Customer Behavior Tracking & Dashboard
## Project Purpose: Built a real-time data pipeline to track and analyze customer behavior on an e-commerce site.

## Project Description:
1. Data Collection : Retrieve customer behavior data via API calls from the backend of the e-commerce website.
2. Data Streaming & Processing:
   + Develop Kafka Producer to publish customer behavior events.
   + Develop Kafka Consumer to subscribe, process, and transform streaming data.
3. Data Storage & Indexing
   + Deploy Elasticsearch for scalable storage and fast retrieval of processed data.
   + Implement a Kafka consumer to push transformed data to Elasticsearch.
4. Data Visualization
   + Set up Kibana using Docker for real-time data visualization.
   + Create an automated dashboard to monitor customer behavior insights.

   
## Project Operation Steps:
1. Install Elasticsearch, Kibana, FastAPI, Kafka via Docker.
2. Define the docker-compose.yaml file for service orchestration.
3. Develop and run Kafka Producer & Consumer for real-time data streaming.
4. Process data using Pandas and push it to Elasticsearch.
5. Design an interactive Kibana dashboard for customer behavior analysis.
   
## Project Expansion:
1. Integrate Apache Airflow to automate data pipeline execution.
2. Improve data processing with advanced streaming analytics
