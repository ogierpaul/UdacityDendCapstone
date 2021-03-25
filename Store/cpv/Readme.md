# CPV (Tendering classification system)
## Dag purpose:  Load transactional data from a local file to Redshift
- From a local csv file to a Resdhift Table

### Source description: Standard CSV
- The source is a local file, as there is no direct url link
- The source data was a bit messy (Excel File) and was reformatted to a standard csv
- As the classification is a fixed convention, this manual way of working is acceptable

### Target description: Redshift table
- The target is a table in the datalake, `cpv_description`

### Dag Steps:
- To ingest it in Redshift, the raw csv file has to be uploaded to S3 first
- Redshift will then copy from S3

#### Redshift processing
- `CREATE` the schema if not exists
- `TRUNCATE` the staging table
- `COPY` the csv file from S3
- `INSERT INTO` the datalake
