Chatbot Interaction Analysis - Big Data Engineering Project

Project Overview

This project processes chatbot interaction data using Apache Spark and integrates with Kafka, PostgreSQL, and Airflow for ETL automation. It performs sentiment analysis on customer service responses and archives parquet files.

Tech Stack:

* Apache Spark (PySpark) for data processing

* Apache Kafka for real-time data streaming

* PostgreSQL for storing sentiment analysis results

* Apache Airflow for orchestration

* S3 storage for archiving

* Python for ETL scripts

Project Structure

1. dags
   
   final_project_archive_files.py  # Airflow DAG to automate ETL steps
3. scripts
   
    LoadFromCsvToJson.py  # Convert CSV data to JSON format
   
    LoadToKafka.py        # Publish JSON data to Kafka topic
   
    WriteToParquetFiles.py # Store data in Parquet format
   
    SentimentAnalysisToPostgres.py # Perform sentiment analysis & save to PostgreSQL
   
    ArchiveFiles.py       # Move processed files to archive
   
    DeleteFiles.py        # Delete files from real time bucket
   
    configuration.py      # Configuration settings for paths, Kafka, DB, etc.
   
5. README.md                 # Project documentation
6. requirements.txt           # Required Python dependencies

Setup & Installation

1. Install Dependencies

  requirements.txt

2. Start Kafka & PostgreSQL (if using Docker)

docker-compose up -d

3. Run ETL Scripts

Execute the scripts in sequence:

python scripts/LoadFromCsvToJson.py
python scripts/LoadToKafka.py
python scripts/WriteToParquetFiles.py
python scripts/SentimentAnalysisToPostgres.py
python scripts/ArchiveFiles.py
python scripts/DeleteFiles.py

4. Schedule with Airflow

Start Airflow and trigger the DAG:

airflow scheduler & airflow webserver

Script Descriptions

1️⃣ LoadFromCsvToJson.py

Reads chatbot interaction CSV files.

Converts them to JSON format for Kafka.

2️⃣ LoadToKafka.py

Publishes JSON data to an Apache Kafka topic.

3️⃣ WriteToParquetFiles.py

Reads data from Kafka.

Stores structured data in Parquet format for further processing.

4️⃣ SentimentAnalysisToPostgres.py

Applies sentiment analysis on chatbot messages.

Stores results in PostgreSQL for reporting.

5️⃣ ArchiveFiles.py

Moves processed files to an archive location (e.g., S3 or local storage).

configuration.py

Stores paths, Kafka topics, database credentials, and other configurations.

Future Enhancements

Implement real-time analytics dashboard.

Add user segmentation based on sentiment trends.

Optimize Kafka consumer processing speed.

Contributors

[Victoria Vilder, Chris Winter, Eliyahu Zinger] (Big Data Engineer)

License

This project is open-source under the MIT License.
