# DECP (Transactions)
## Dag purpose:  Load transactional data from a website to Redshift
- From an internet file to a Resdhift Table

### Source description: Standard JSON
- The source is a download url
- The downloaded file is a standard formatted JSON file

### Target description: Redshift tables
- The target is two redshift tables in the datalake, `decp_marches` and `decp_titulaires`
- One is about the tender itself (marches), the other one about the winner (titulaires)

### Dag Steps:
- To ingest it in Redshift, the raw JSON file has to be:
    - downloaded from a URL
    - processed from a standard JSON to a JSON lines format
    - normalized
- Redshift is not able to perform those steps
- It is thus necessary to use an EC2 instance to perform them and output the results to S3
- Redshift will then copy from S3

#### EC2 processing
- Start EC2 instance
- Install `jq`, a command line tool to process JSON files to JSON lines 
    - [https://stedolan.github.io/jq/manual/](https://stedolan.github.io/jq/manual/)
- Upload some configuration files (`JSON Path` files)
    - First from the Airflow local folder to S3 using `s3client.upload`
    - Then copy from S3 to EC2 using `aws s3 cp`
    - This two-step approach is easier because S3 is good  at moving files
- Download from the Web using `wget`
- Process the file from JSON to JSON Lines using `jq`
- Extract marches and titulaires information from the JSON Lines
    - This will create a normalized data model, ingestible by Redshift
- Copy the results to S3 using `aws s3 cp`
- Stop the EC2 instance

#### Redshift processing
- `CREATE` the schema if not exists
- `TRUNCATE` the staging tables
- `COPY` the two files from S3
- `INSERT INTO` the datalake

## Dags Parameters and code
### EC2 Config
- `dict` in the dag file
- contains parameters of the EC2 machine to be created

### DECP Config
- `dict` in the dag file
- contains information on the bucket where to stage the files
- And the url to download the data

### Code
#### 1_download_from_web
- Contains a series of bash statements to be executed by the EC2 machine

#### 2_transform_in_redshift
- Contains SQL statements to be executed by Redshift