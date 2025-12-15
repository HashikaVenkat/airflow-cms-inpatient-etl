Overview

This repository contains a containerized ETL pipeline built using Apache Airflow to ingest, transform, and load Medicare inpatient claims data into a PostgreSQL database. The pipeline uses publicly available CMS Synthetic Public Use Files (SynPUF) and produces an analytics-ready table by joining inpatient claims with beneficiary data.

The project is designed to reflect a production-style workflow, including orchestration, staging tables, data quality checks, and idempotent runs.

Industry Context

Domain: Healthcare analytics

Data source: Centers for Medicare & Medicaid Services (CMS)

Dataset: Medicare Synthetic Public Use Files (SynPUF)

Data type: Inpatient claims and beneficiary summary data

Coding system: ICD-9 diagnosis codes

The dataset is synthetic and intended for research and educational use.


Technology Stack

Apache Airflow (workflow orchestration)

Docker and Docker Compose (containerized execution)

PostgreSQL (data storage and transformations)

Python (ETL logic)

CMS public data endpoint (data ingestion)



Pipeline Description
DAG 1: cms_inpatient_download_unzip

Purpose: Data ingestion and staging

Downloads CMS inpatient claims ZIP file from the CMS website

Validates download integrity

Extracts compressed files

Stages CSV files within the Airflow container volume


DAG 2: patient_claims_plus_postgres

Purpose: Load, transform, and validate data in PostgreSQL

Drops the final table if it exists to allow repeatable runs

Creates raw staging tables for inpatient claims and beneficiary data

Loads selected columns from CSV files into staging tables

Performs row-count data quality checks on staging tables

Joins claims and beneficiary data on patient identifier

Creates a final table (patient_claims_plus) for analytical use

Performs a final data quality check on the output table



Output Table

The final table patient_claims_plus includes:

Patient identifier

Claim start and end dates

Claim payment amount

Provider identifier

ICD-9 diagnosis codes

Patient demographic attributes and insurance coverage details

The table is suitable for downstream analytical use cases such as utilization analysis, cost analysis, and population-level studies.

Author

Hashika Venkatramanan
