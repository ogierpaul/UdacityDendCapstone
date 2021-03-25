from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from ec2operators import Ec2BashExecutor, Ec2Creator, Ec2Terminator
from airflow.configuration import conf
from redshiftoperators import RedshiftCopyFromS3, RedshiftOperator, RedshiftUpsert
from airflow.operators.dummy import DummyOperator
import os

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
    'region_name': 'eu-central-1',
    'autocommit': True,
    'tag_key': 'Stream',
    'tag_value': 'Siren', #TODO: Review
    'execution_timeout': timedelta(seconds=300),
    's3_bucket': Variable.get('s3_bucket'),
    'arn': Variable.get('arn'),
    'working_dir': os.path.dirname(os.path.abspath(__file__))
}

ec2_config = {
    'ImageId': 'ami-0de9f803fcac87f46',
    'InstanceType': 't2.medium',
    'KeyName': 'ec2keypairfrankfurt',
    'IamInstanceProfileName': 'myec2ssms3',
    'SecurityGroupId': 'sg-c21b9eb8',
    'start_sleep': 60
}

with DAG(
    'infogreffe_from_web_to_redshift',
    default_args=default_args,
    description='Download the Infogreffe File from the API to Redshift',
    schedule_interval=None,
    tags=['dend', 'infogreffe']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_infogreffe = DummyOperator(
        task_id='start_infogreffe'
    )

    stop_infogreffe = DummyOperator(
        task_id='stop_infogreffe'
    )

    create_ec2_if_not_exists = Ec2Creator(
        task_id='create_ec2_if_not_exists',
        **ec2_config
    )

    download_from_api_to_s3 = Ec2BashExecutor(
        task_id='download_from_api_to_s3',
        bash='1_ec2_instructions.sh',
        sleep=5,
        retry=30
    )

    stop_ec2 = Ec2Terminator(
        task_id='stop_ec2',
        terminate='stop',
        #TODO: Remove trigger_rule='all_done'
    )

    create_redshift = RedshiftOperator(
        task_id='create_redshift',
        dag=dag,
        sql='2_create_redshift.sql'
    )

    copy_from_s3 = RedshiftCopyFromS3(
        task_id='copy_from_s3',
        s3_folder='staging/infogreffe_attributes',
        fn=Variable.get('infogreffe_csvname'),
        schema='staging',
        table='infogreffe_attributes',
        format='csv',
        header=True,
        delimiter=';'
    )
    upsert_datalake = RedshiftUpsert(
        task_id='upsert_datalake',
        schema="datalake",
        table="siren_attributes",
        pkey="siren",
        sql="SELECT * FROM staging.infogreffe_attributes" #TODO: Review if no duplicates
    )

    start_infogreffe >> create_ec2_if_not_exists >> download_from_api_to_s3 >> stop_ec2
    stop_ec2 >> create_redshift >> copy_from_s3 >> upsert_datalake >> stop_infogreffe
