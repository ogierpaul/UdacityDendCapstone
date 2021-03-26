# Dags Repository
## Purpose
- Store Dags to be Run in Airflow

## Initialization Files
- Create the connections
- Create the Variables
- Create the schemas in Redshift 
- #TODO: Review and complete

## Ingestion and Transformation Dags
- 4 Folders `cpv`, `decp`, `infogreffe`, `siren`
- One per each data source
- Contains the steps needed to :
    - Download the data to S3
    - Ingest that data in Redshift in the Staging Layer
    - Upsert that data in the Datalake layer
    - Transform, normalize that data in the Data Warehouse Layer

 