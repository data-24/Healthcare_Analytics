# Healthcare Analytics

     This project aims to build a robust and scalable data pipeline for healthcare data using AWS S3, Snowflake, dbt, Airflow, and Cortex AI. The pipeline follows a Medallion architecture (Bronze, Silver, Gold) for data processing and incorporates data quality checks to ensure data integrity.

     ## Project Structure

     - `models/`: Contains dbt models for data transformation
       - `bronze/`: Raw data staging models
       - `silver/`: Transformed and cleaned data models
       - `gold/`: Business-ready data marts
     - `tests/`: Contains data quality tests for dbt models
     - `dags/`: Contains Airflow DAG definitions
     - `snowpark/`: Contains Snowpark Python scripts for data processing
     - `cortex/`: Contains Cortex AI configuration and scripts

     ## Getting Started

     [Instructions on how to set up and run the project locally]

     ## Dependencies

     - dbt
     - Apache Airflow
     - Snowpark Python
     - Cortex AI

     ## Contributors

     - Priyanka Pandey
     

     ## License

     [Choose an appropriate license for your project, e.g., MIT, Apache 2.0]