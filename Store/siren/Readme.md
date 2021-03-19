# Siren (Company national register)
## Dag purpose:  Load transactional data from a website to Redshift
- From an internet file to a Resdhift Table

### Source description: Standard JSON
- The source is a download url
- The downloaded file is a compressed zipped csv

### Target description: Redshift table
- The target is a table in the datalake, `siren_attribtues`

### Dag Steps:
- To ingest it in Redshift, the raw zipped file has to be:
    - downloaded from a URL
    - unzipped
- Redshift is not able to perform those steps
- It is thus necessary to use an EC2 instance to perform them and output the results to S3
- Redshift will then copy from S3

#### EC2 processing
- Start EC2 instance
- Download from the Web using `wget`
- `unzip` the file
- Copy the results to S3 using `aws s3 cp`
- Stop the EC2 instance

#### Redshift processing
- `CREATE` the schema if not exists
- `TRUNCATE` the staging table
- `COPY` the csv file from S3
- `INSERT INTO` the datalake
