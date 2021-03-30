# Plugins Developped
## Purpose
- It is best for maintenance and readability to limit the use of plugins and only use community Airflow Operators
- However, it is sometimes necessary to develop custom plugins

## Ec2 Operators
- Set of plugins interacting with Ec2 Instances
    - Create an Ec2 Instance
    - Execute Bash Commands on an EC2 Instance
    - Terminate an Ec2 Instance

## Redshift Operators
- Based on PostgresOperator
    - Execute SQL queries or SQL scripts
    - Copy data from S3
    - Upsert data based on a primary key
    - Perform Data Quality Checks
    
## S3 Uploader
- Upload a local file to S3