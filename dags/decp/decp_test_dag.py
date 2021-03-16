from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import Ec2BashExecutor, BaseEc2Operator, Ec2Creator, Ec2Terminator, S3UploadFromLocal
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
    'tag_value': 'Siren',
    'execution_timeout': timedelta(seconds=300),
}
dag = DAG(
    'decp_test_dag',
    default_args=default_args,
    description='Decp Dag',
    schedule_interval=None
)

upload = S3UploadFromLocal(
    task_id='DummyOperator',
    dag=dag,
    fp='/usr/local/airflow/dags/decp/hello.txt',
    s3_bucket='paulogiereucentral1',
    s3_key='p6/hello.txt'
)