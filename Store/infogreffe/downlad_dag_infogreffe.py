from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from ec2operators import Ec2BashExecutor, Ec2CurlGet, Ec2Creator, Ec2Terminator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from redshiftoperators import RedshiftUpsert, RedshiftCopyFromS3, RedshiftOperator
from airflow.operators.dummy import DummyOperator
import os




dag_folder = '/Users/paulogier/81-GithubPackages/UdacityDendCapstone/dags/'


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'redshift_conn_id': 'aa_redshift',
    'aws_conn_id': 'aws_credentials',
    'autocommit': True,
    'tag_key': 'Stream',
    'tag_value': 'Infogreffe',
    'execution_timeout': timedelta(seconds=300)
}

with DAG(
    'test_infogreffe_api_download',
    default_args=default_args,
    description='Download the Infogreffe data from the API to Redshift',
    schedule_interval=None,
    tags=['dend']
) as dag:
    execute_script = Ec2BashExecutor(
        task_id='copy_to_s3',
        bash=os.path.join(dag_folder, '3_copy_to_s3.sh'),
        parameters=infogreffe_config,
        sleep=10,
        retry=20
    )









