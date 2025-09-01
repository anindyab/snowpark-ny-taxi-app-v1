# snowpark-ny-taxi-app-v1
Snowpark app to load NY Taxi cab ride data and store in fact and dimension tables

## 📦 Requirements

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page stores TLC Trip Record Data. Yellow and green taxi trip records include fields capturing pickup and drop-off dates/times, pickup and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The goal is to ingest and process the data as per the medallion architecture (https://www.databricks.com/glossary/medallion-architecture) in a snowflake snowpark project (https://docs.snowflake.com/en/developer-guide/snowpark/index).  

## 📦 Medallion architecture

The Medallion architecture organizes data into Bronze (raw), Silver (cleansed), and Gold (consumption-ready) layers to incrementally improve data quality and structure. Fact and dimension tables are primarily built and utilized in the Gold layer, which contains de-normalized, read-optimized data models designed for specific business analytics, though some structuring may begin in Silver. 
Bronze Layer: The Raw Zone
Purpose: To ingest and store raw, untransformed data directly from source systems. 
Data Characteristics: A one-to-one copy of source data, it serves as an historical archive but lacks quality checks and matching with other data. 
Fact/Dimension Table Status: No fact or dimension tables are typically present in this layer. 
Silver Layer: The Cleansed Zone
Purpose:
.
To cleanse, filter, and enrich the raw data from the Bronze layer into structured, high-quality datasets. 
Data Characteristics:
.
Data is cleaned, validated, and potentially joined or denormalized. Type 2 slowly changing dimensions are often handled here. 
Fact/Dimension Table Status:
.
Some basic structuring or joining might begin, but it's not the primary layer for final fact and dimension tables. 
Gold Layer: The Consumption-Ready Zone 
Purpose:
.
To create project-specific, de-normalized data models ready for reporting and business intelligence.
Data Characteristics:
.
Highly refined, read-optimized data models optimized for analytical queries with fewer joins. This layer contains aggregated and business-specific data.
Fact/Dimension Table Status:
.
This is where the final fact and dimension tables, often in a star schema, are built to support analytics and reporting.
How Fact and Dimension Tables Fit In
Fact Tables: Store metrics and measurements (facts) about a business process. 
Dimension Tables: Store descriptive attributes (dimensions) that provide context to the facts. 
Location: Both are created and reside in the Gold layer to support reporting and analytics by providing a clear, business-oriented view of the data. 


# Design

This repository contains Python scripts to:
- Create **Snowflake Task DAGs** - (a workflow with parent and child tasks for the data pipeline).
- The implementation reference is https://docs.snowflake.com/en/developer-guide/snowpark/python/index
- Build and merge a **Rate Code Dimension** table using **Snowpark for Python**. Read https://github.com/anindyab/snowpark-ny-taxi-app-v1/blob/devenv/create_task.py for details.
- The root level task is NIGHTLY_ROOT_TASK that runs at 2 am and calls a bronze_ingest_procedure
- Upon completion this triggers SILVER_CLEAN_TASK that calls silver_clean_procedure
- The final task in the workflow is GOLD_MODEL_TASK that calls gold_model_procedure 
- https://github.com/anindyab/snowpark-ny-taxi-app-v1/blob/devenv/ny_taxi_ride/app/procedures.py describes these procedures. The code is suitably commented
- The DDL for all tables is in https://github.com/anindyab/snowpark-ny-taxi-app-v1/blob/devenv/ddl.sql

# Vault
We have used the following
- Repository secrets to store account, user, password, warehouse, database, schema and role in order to build and deploy artifacts from local to dev,qa, and prod environments (see https://github.com/anindyab/snowpark-ny-taxi-app-v1/blob/devenv/.github/workflows/build_and_deploy.yaml)
- Github codespaces secrets with same variables to store these in the deevelopment environment


---

## 🚀 Features
- Automates creation of parent and child tasks in Snowflake.
- Demonstrates DAG dependencies between tasks.
- Implements dimension loading pattern with Snowpark:
  - Reads distinct values from a source table.
  - Maps codes to human-readable names.
  - Performs `MERGE` into dimension table.
  - Logs loads into a tracking table.

---

## 📦 Implementation

- Python 3.9+  
- This was built using a conda environment (conda create --name py39_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.9 numpy pandas pyarrow
)
- Snowflake account with the following enabled:
  - [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
  - Task & stored procedure privileges

### Python Dependencies
Install required libraries:
```bash
pip install -r requirements.txt

