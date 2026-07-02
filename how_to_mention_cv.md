## End-to-End Healthcare Data Pipeline
**Snowflake · dbt · Apache Airflow · AWS S3 · Python** — *Personal Project*

- Built a production-grade healthcare analytics pipeline ingesting multi-source data (admissions, treatments, claims) from AWS S3 into Snowflake, orchestrated end-to-end with Apache Airflow.
- Engineered a Python/Snowpark "gatekeeper" that runs 12 tiered data-quality checks on every file before load, automatically quarantining bad files and dispatching email alerts, preventing invalid data from reaching the warehouse.
- Designed a dbt medallion architecture (Bronze → Silver → Gold) with a star schema, SCD Type-2 history tracking, and declared PK/FK relationships, transforming 150K+ records into analytics-ready dimensional models.
- Implemented automated monitoring with root-cause alerting and full audit logging, plus a Snowflake Cortex Analyst semantic layer enabling natural-language querying of the data.
