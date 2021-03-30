from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from ec2operators import Ec2BashExecutor, Ec2Creator, Ec2Terminator
from s3uploader import S3UploadFromLocal
from redshiftoperators import RedshiftCopyFromS3, RedshiftOperator, RedshiftUpsert, RedshiftQualityCheck
import os

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
    'tag_value': 'Unique',
    'execution_timeout': timedelta(seconds=300),
    's3_bucket': Variable.get('s3_bucket'),
    'arn': Variable.get('arn'),
    'working_dir' : os.path.dirname(os.path.abspath(__file__))
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
    'decp_test_wo_comment',
    default_args=default_args,
    description='Download DECP data from Web to Redshift',
    schedule_interval=None,
    tags=['dend', 'decp', 'staging']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()


    create_schema = RedshiftOperator(
        task_id='create_decp_schema',
        sql='schema_decp_staging_datalake.sql'
    )

    copy_titulaires_from_s3 = RedshiftCopyFromS3(
        task_id='copy_titulaires_from_s3',
        s3_folder='staging/decp_titulaires',
        fn='decp_titulaires.json',
        schema='staging',
        table='decp_titulaires',
        format='json',
        jsonpath='auto'
    )

    copy_marches_from_s3 = RedshiftCopyFromS3(
        task_id='copy_marches_from_s3',
        s3_folder='staging/decp_marches',
        fn='decp_marches.json',
        schema='staging',
        table='decp_marches',
        format='json',
        jsonpath='auto'
    )

    upsert_titulaires = RedshiftUpsert(
        task_id='upsert_titulaires',
        sql="SELECT * FROM staging.decp_titulaires_unique;",
        schema='datalake',
        table='decp_titulaires',
        pkey='decp_bridge_uid'
    )

    upsert_marches = RedshiftUpsert(
        task_id='upsert_marches',
        sql="SELECT * FROM staging.decp_marches_unique;",
        schema='datalake',
        table='decp_marches',
        pkey='decp_uid'
    )

    q_check_marches = RedshiftQualityCheck(
        task_id='quality_check_marches',
        schema="datalake",
        table="decp_marches",
        pkey="decp_uid"
    )

    q_check_titulaires = RedshiftQualityCheck(
        task_id='quality_check_titulaires',
        schema="datalake",
        table="decp_titulaires",
        pkey="decp_bridge_uid"
    )

    stop_decp = DummyOperator(
        task_id='Stop_decp'
    )


create_schema >> [copy_titulaires_from_s3, copy_marches_from_s3]
copy_marches_from_s3 >> upsert_marches >> q_check_marches
copy_titulaires_from_s3 >> upsert_titulaires >> q_check_titulaires
[q_check_marches, q_check_titulaires] >> stop_decp
