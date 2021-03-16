from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators import RedshiftUpsert

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
    'tag_value': 'Decp',
    'execution_timeout': timedelta(seconds=300),
}

decp_config = {
    'bucket': 'paulogiereucentral1'
}
script_folder = 'decp/2_transform_in_redshift/'

dag = DAG(
    'decp_redshift_dag',
    default_args=default_args,
    description='Staging Data from S3 to Redshift Datalake',
    schedule_interval=None
)

create_redshift_schema = PostgresOperator(
    task_id='create_redshift_schema',
    dag=dag,
    sql='1_create_schema.sql'
    )

truncate_marches = PostgresOperator(
    task_id='truncate_staging_marches',
    dag=dag,
    sql="TRUNCATE staging.decp_marches;"
)

copy_marches_from_s3 = S3ToRedshiftTransfer(
    task_id='copy_marches_from_s3',
    dag=dag,
    schema='staging',
    table='decp_marches',
    s3_bucket=decp_config['bucket'],
    s3_key='staging',
    copy_options=["FORMAT AS JSON 'auto' "]
)

truncate_titulaires = PostgresOperator(
    task_id='truncate_staging_titulaires',
    dag=dag,
    sql="TRUNCATE staging.decp_titulaires;"
)

copy_titulaires_from_s3 = S3ToRedshiftTransfer(
    task_id='copy_titulaires_from_s3',
    dag=dag,
    schema='staging',
    table='decp_titulaires',
    s3_bucket=decp_config['bucket'],
    s3_key='staging',
    copy_options=["FORMAT AS JSON 'auto'"]
)

create_redshift_schema >> truncate_marches >> copy_marches_from_s3
create_redshift_schema >> truncate_titulaires >> copy_titulaires_from_s3

