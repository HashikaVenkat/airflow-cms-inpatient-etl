Overview

This repository contains a containerized ETL pipeline implemented using Apache Airflow to ingest, transform, and load Medicare inpatient claims data into a PostgreSQL database. The pipeline leverages publicly available CMS Synthetic Public Use Files (SynPUF) and produces an analytics-ready dataset by joining inpatient claims data with beneficiary information. The overall design reflects a production-style workflow, incorporating orchestration, raw data staging, explicit data quality checks, and idempotent execution to support repeatable and reliable pipeline runs.

Industry Context

The project is situated within the healthcare analytics domain and uses data published by the Centers for Medicare & Medicaid Services (CMS). The dataset consists of Medicare Synthetic Public Use Files, which include inpatient claims records and beneficiary summary data. These files are synthetic and de-identified, making them suitable for research and educational purposes. The claims data uses ICD-9 diagnosis codes to represent medical conditions associated with inpatient encounters.

Technology Stack

The pipeline is orchestrated using Apache Airflow, which manages task dependencies and execution flow. Docker and Docker Compose are used to provide a containerized runtime environment, ensuring consistency and reproducibility across executions. PostgreSQL serves as the target database for staging and final transformed tables, while Python is used to implement the ETL logic. Data ingestion is performed directly from a public CMS data endpoint.

Pipeline Description

The pipeline is composed of two Airflow DAGs. The first DAG, cms_inpatient_download_unzip, is responsible for data ingestion and staging. It downloads the CMS inpatient claims ZIP file from the CMS website, validates the integrity of the download, extracts the compressed contents, and stages the resulting CSV files within the Airflow container volume for downstream processing.

The second DAG, patient_claims_plus_postgres, handles loading, transformation, and validation of the data within PostgreSQL. It begins by dropping the final output table if it already exists to support idempotent execution. The DAG then creates raw staging tables for inpatient claims and beneficiary data, loads selected columns from the staged CSV files, and performs row-count data quality checks to ensure successful ingestion. After validation, the claims and beneficiary datasets are joined on the patient identifier to produce a consolidated analytics table. A final data quality check confirms that the output table has been populated successfully.

Output Table

The final output table, patient_claims_plus, contains patient-level inpatient claim records enriched with beneficiary attributes. It includes patient identifiers, claim start and end dates, claim payment amounts, provider identifiers, ICD-9 diagnosis codes, and patient demographic and insurance coverage details. The resulting table is structured for downstream analytical use cases such as healthcare utilization analysis, cost analysis, and population-level studies.

Author

Hashika Venkatramanan
