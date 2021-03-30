# AA Setup of Airflow
## Purpose
- Set up airflow configuration before launching the dags

## Steps
- Create the Redshift Cluster
- Run the `1_create_schemas.sql` statements within the redshift cluster
- Fill inside the set_variable.py the different variables needed, including cluster parameters
- Fill inside of Airflow the Redshift Connection (Postgre Hook) and Aws Credentials (AwsHook)