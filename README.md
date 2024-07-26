# An End-to-End Airflow Data Pipeline with BigQuery and dbt

## Table of Contents
- [Introduction](#introduction)
- [Architecture](#architecture)
- [Setup](#setup)
- [Airflow Pipeline](#airflow-pipeline)
- [DBT Transformation](#dbt-transformation)
- [Dashboard Creation](#dashboard-creation)


## Introduction
**An End-to-End Airflow Data Pipeline with BigQuery and dbt** 
is a comprehensive data engineering project that automates 
the process of extracting data from a local device, uploading it to Google Cloud Storage, loading it into BigQuery, transforming the data using dbt to create dimension and fact tables, and finally visualizing the data through a dashboard.
## Architecture
1. **Data Extraction**: Data is extracted from the local device.
2. **Data Upload**: The extracted data is uploaded to Google Cloud Storage.
3. **Data Loading**: The uploaded data is then loaded into BigQuery.
4. **Data Transformation**: dbt is used to create dimension and fact tables in the BigQuery data warehouse.
5. **Data Visualization**: The transformed data is used to create a dashboard.

![Architecture Diagram](airflow_dbt.png)

## Setup
1. **Prerequisites**:
   - Python 3.x
   - Google Cloud SDK
   - Apache Airflow
   - dbt
   - BigQuery
   - Google Cloud Storage

