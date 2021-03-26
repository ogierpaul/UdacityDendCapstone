from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from ec2operators import Ec2BashExecutor, Ec2Creator, Ec2Terminator
from s3uploader import S3UploadFromLocal
from redshiftoperators import RedshiftCopyFromS3, RedshiftOperator, RedshiftUpsert
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
    'decp_web_to_redshift_dag',
    default_args=default_args,
    description='Download DECP data from Web to Redshift',
    schedule_interval=None,
    tags=['dend', 'decp']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_decp = DummyOperator(
        task_id='Start_decp'
    )
    stop_decp = DummyOperator(
        task_id='Stop_decp'
    )
    upload_config_marches = S3UploadFromLocal(
        task_id='Upload_config_marches',
        fn='jq_marches.sh',
        s3_folder='config/'
    )
    upload_config_titulaires = S3UploadFromLocal(
        task_id='Upload_config_titulaires',
        fn='jq_titulaires.sh',
        s3_folder='config/'
    )
    create_ec2 = Ec2Creator(
        task_id='decp_create_ec2',
        retry=30,
        sleep=5,
        **ec2_config
    )

    download_extract_copy_file = Ec2BashExecutor(
        task_id='decp_download_extract_copy_file',
        dag=dag,
        bash='ec2_commands.sh ',
        sleep=10,
        retry=30
    )

    stop_ec2 = Ec2Terminator(
        task_id='stop_ec2',
        dag=dag,
        terminate='stop',
        trigger_rule='all_done'
    )

    create_schema = RedshiftOperator(
        task_id='create_decp_schema',
        sql='1_create_schema.sql'
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
        sql='2_select_unique_decp_titulaires.sql',
        schema='datalake',
        table='decp_titulaires',
        pkey='decp_bridge_uid'
    )

    upsert_marches = RedshiftUpsert(
        task_id='upsert_marches',
        sql='2_select_unique_decp_marches.sql',
        schema='datalake',
        table='decp_marches',
        pkey='decp_uid'
    )


start_decp >> [upload_config_marches, upload_config_titulaires] >> create_ec2 >> download_extract_copy_file >> stop_ec2
stop_ec2 >> create_schema >> [copy_titulaires_from_s3, copy_marches_from_s3]
copy_marches_from_s3 >> upsert_marches
copy_titulaires_from_s3 >> upsert_titulaires
[upsert_marches, upsert_titulaires] >> stop_decp
