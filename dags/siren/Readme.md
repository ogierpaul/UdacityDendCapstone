# Siren (Company national register)
## Dag purpose:  Load transactional data from a website to Redshift
- From an internet file to a Resdhift Table

### Source description: Standard JSON
- The source is a download url
- The downloaded file is a compressed zipped csv

### Target description: Redshift table
- The target is a table in the datalake, `siren_attributes`

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

## Dags Parameters and code
### EC2 Config
- `dict` in the dag file
- contains parameters of the EC2 machine to be created

### Airflow Variables
- `s3_bucket` : contains information on the bucket where to stage the files
- `siren_url`: url to download the data
- `siren_csvname`: filename of csv contained within zip file
- `arn`: ARN identifier of the role to be assumed by Redshift to `COPY` the file from S3

### Airflow connections
- `aa_redshift` : Postgres Connection
- `aws_credentials`: AWS Connection

### Code
#### 1_siren_ec2_instructions.sh
- list of instructions to be executed by ec2 machine

#### 2_create_redshift.sql
- Creation of Redshift schema for this dag
