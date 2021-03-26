# Infogreffe (Financial statements from companies)
## Dag purpose:  Load attributes data from an API to Redshift
- From the Infogreffe API

### Source description: API
- The source is an API
    - The description is at [here](https://opendata.datainfogreffe.fr/api/v1/console/datasets/1.0/search/)
- We select the fields we want to upload in the api url
- The source is updated once every quarter (when the financial statements are due)

### Target description: Redshift table
- The target is a table in the datalake, `infogreffe_attributes`

### Dag Steps:
- To ingest it in Redshift, the data must be downloaded from the API
    - an EC2 instance query the API using `curl` and copy the results from S3
- Redshift will then copy from S3

#### Redshift processing
- `CREATE` the schema if not exists
- `TRUNCATE` the staging table
- `COPY` the csv file from S3
- `INSERT INTO` the datalake
    - the table is normalized first
    - filter out rows with correct year formatting on column `millesime`
