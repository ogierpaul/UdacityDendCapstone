from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import Ec2BashExecutor, BaseEc2Operator, Ec2Creator, Ec2Terminator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators import RedshiftUpsert

ec2_config = {
    'ImageId': 'ami-0de9f803fcac87f46',
    'InstanceType': 't2.medium',
    'KeyName': 'ec2keypairfrankfurt',
    'IamInstanceProfileName': 'myec2ssms3',
    'SecurityGroupId': 'sg-c21b9eb8',
    'StartSleep': 60
}
siren_config = {
    'url': 'https://www.data.gouv.fr/en/datasets/r/573067d3-f43d-4634-9664-675277b81857',
    'csvname': 'StockUniteLegale_utf8.csv',
    'output_s3': 's3://paulogiereucentral1/staging/staging_siren/StockUniteLegale_utf8.csv'
}

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'postgres_conn_id': 'aa_redshift',
    'redshift_conn_id': 'aa_redshift',
    'aws_conn_id': 'aws_credentials',
    'aws_credentials': 'aws_credentials',
    'autocommit': True,
    'tag_key': 'Stream',
    'tag_value': 'Siren',
    'execution_timeout': timedelta(seconds=300),
}
dag = DAG(
    'siren_complete',
    default_args=default_args,
    description='Siren dags for EC2 and Redshift',
    schedule_interval=None
)

create_ec2_if_not_exists = Ec2Creator(
    task_id='create_ec2_if_not_exists',
    dag=dag,
    **ec2_config
)

prepare_ec2 = Ec2BashExecutor(
    task_id='prepare_ec2',
    dag=dag,
    sh='/usr/local/airflow/dags/siren/1_siren_ec2_init.sh'
)

download_from_web = Ec2BashExecutor(
    task_id='download_from_web',
    dag=dag,
    sh='/usr/local/airflow/dags/siren/2_siren_web_download.sh',
    parameters=siren_config,
    sleep=5,
    retry=20
)

copy_to_s3 = Ec2BashExecutor(
    task_id='copy_to_s3',
    dag=dag,
    sh='/usr/local/airflow/dags/siren/3_copy_to_s3.sh',
    parameters=siren_config,
    sleep=10,
    retry=20
)

stop_ec2 = Ec2Terminator(
    task_id='stop_ec2',
    dag=dag,
    terminate='stop'
)

create_redshift = PostgresOperator(
    task_id='create_redshift',
    dag=dag,
    sql='siren/siren_create.sql'
)

truncate_staging = PostgresOperator(
    task_id='truncate_staging',
    dag=dag,
    sql="TRUNCATE staging.staging_siren;"
)

copy_from_s3 = S3ToRedshiftTransfer(
    task_id='copy_from_s3',
    dag=dag,
    schema='staging',
    table='staging_siren',
    s3_bucket='paulogiereucentral1',
    s3_key='p6/staging',
    copy_options=["FORMAT AS CSV", "IGNOREHEADER AS 1", "DELIMITER AS ','"]
)

upsert_datalake = RedshiftUpsert(task_id='upsert_datalake',
                                 dag=dag,
                                 conn_id='aa_redshift',
                                 pkey="siren",
                                 query="SELECT * FROM staging.staging_siren",
                                 table="siren_attributes",
                                 schema="datalake")

create_ec2_if_not_exists >> prepare_ec2 >> download_from_web >> copy_to_s3 >> stop_ec2
stop_ec2 >> create_redshift >> truncate_staging >> copy_from_s3 >> upsert_datalake
