from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from ec2operators import Ec2BashExecutor, Ec2Creator, Ec2Terminator
from s3uploader import S3UploadFromLocal
from redshiftoperators import RedshiftCopyFromS3, RedshiftOperator, RedshiftUpsert
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
# from airflow.operators import RedshiftUpsert
from airflow.operators.dummy import DummyOperator

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
    'arn': 'arn:aws:iam::075227836161:role/redshiftwiths3'
}
ec2_config = {
    'ImageId': 'ami-0de9f803fcac87f46',
    'InstanceType': 't2.medium',
    'KeyName': 'ec2keypairfrankfurt',
    'IamInstanceProfileName': 'myec2ssms3',
    'SecurityGroupId': 'sg-c21b9eb8',
    'start_sleep': 60
}
decp_config = {
    'bucket': 'paulogiereucentral1',
    'url': 'https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2'
}

dag = DAG(
    'decp_from_web_to_redshift',
    default_args=default_args,
    description='Download DECP data from Web to Redshift',
    schedule_interval=None,
    tags=['dend']
)
dir_web_scripts = '/Users/paulogier/81-GithubPackages/UdacityDendCapstone/dags/decp/1_download_from_web/'
dir_transform_sql = '/Users/paulogier/81-GithubPackages/UdacityDendCapstone/dags/decp/2_transform_in_redshift/'
_docs_md_fp = 'Users/paulogier/81-GithubPackages/UdacityDendCapstone/dags/decp/Readme.md'
dag.doc_md = open(_docs_md_fp, 'r').read()

start_decp = DummyOperator(
    task_id='Start_decp',
    dag=dag
)
stop_decp = DummyOperator(
    task_id='Stop_decp',
    dag=dag,
    trigger_rule='all_done'
)

upload_config_marches = S3UploadFromLocal(
    task_id='Upload_config_marches',
    dag=dag,
    fp=dir_web_scripts + 'jq_marches.sh',
    s3_bucket=decp_config['bucket'],
    s3_key='config/jq_marches.sh'
)

upload_config_titulaires = S3UploadFromLocal(
    task_id='Upload_config_titulaires',
    dag=dag,
    fp=dir_web_scripts + 'jq_titulaires.sh',
    s3_bucket=decp_config['bucket'],
    s3_key='config/jq_titulaires.sh'
)

create_ec2 = Ec2Creator(
    task_id='decp_create_ec2',
    dag=dag,
    retry=20,
    sleep=5,
    **ec2_config
)

prepare_ec2 = Ec2BashExecutor(
    task_id='decp_prepare_ec2',
    dag=dag,
    sh=dir_web_scripts + '1_prepare_ec2.sh',
    parameters=decp_config
)

download_from_web = Ec2BashExecutor(
    task_id='decp_download_from_web',
    dag=dag,
    sh=dir_web_scripts + '2_download_from_web.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

create_json_temp = Ec2BashExecutor(
    task_id='decp_create_json_temp',
    dag=dag,
    sh=dir_web_scripts + '3_create_jq_temp.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

extract_marches = Ec2BashExecutor(
    task_id='decp_extract_marches',
    dag=dag,
    sh=dir_web_scripts + '4_extract_marches.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

extract_titulaires = Ec2BashExecutor(
    task_id='decp_extract_titulaires',
    dag=dag,
    sh=dir_web_scripts + '4_extract_titulaires.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

copy_titulaires_to_s3 = Ec2BashExecutor(
    task_id='copy_titulaires_to_s3',
    dag=dag,
    sh=dir_web_scripts + '5_copy_titulaires.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

copy_marches_to_s3 = Ec2BashExecutor(
    task_id='copy_marches_to_s3',
    dag=dag,
    sh=dir_web_scripts + '5_copy_marches.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

clean_ec2 = Ec2BashExecutor(
    task_id='clean_ec2',
    dag=dag,
    sh=dir_web_scripts + '6_clean_ec2.sh',
    parameters=decp_config
)

stop_ec2 = Ec2Terminator(
    task_id='stop_ec2',
    dag=dag,
    terminate='stop',
    trigger_rule='all_done'
)

create_redshift_schema = RedshiftOperator(
    task_id='create_redshift_schema',
    dag=dag,
    sql=dir_transform_sql + '1_create_schema.sql'
)

truncate_marches = RedshiftOperator(
    task_id='truncate_staging_marches',
    dag=dag,
    sql="TRUNCATE staging.decp_marches;"
)

copy_marches_from_s3 = RedshiftCopyFromS3(
    task_id='copy_marches_from_s3',
    dag=dag,
    schema='staging',
    table='decp_marches',
    s3path='s3://paulogiereucentral1/staging/decp_marches/',
    format='json',
    jsonpath='auto',
    truncate=True
)

truncate_titulaires = RedshiftOperator(
    task_id='truncate_staging_titulaires',
    dag=dag,
    sql="TRUNCATE staging.decp_titulaires;"
)

copy_titulaires_from_s3 = RedshiftCopyFromS3(
    task_id='copy_titulaires_from_s3',
    dag=dag,
    schema='staging',
    table='decp_titulaires',
    s3path='s3://paulogiereucentral1/staging/decp_titulaires/',
    format='json',
    jsonpath='auto',
    truncate=True
)

start_decp >> [upload_config_titulaires, upload_config_marches] >> create_ec2
create_ec2 >> prepare_ec2 >> download_from_web >> create_json_temp
create_json_temp >> [extract_marches, extract_titulaires]
extract_marches >> copy_marches_to_s3
extract_titulaires >> copy_titulaires_to_s3
[copy_marches_to_s3, copy_titulaires_to_s3] >> clean_ec2
clean_ec2 >> stop_ec2 >> create_redshift_schema
create_redshift_schema >> [truncate_marches, truncate_titulaires]
truncate_marches >> copy_marches_from_s3
truncate_titulaires >> copy_titulaires_from_s3
[copy_marches_from_s3, copy_titulaires_from_s3] >> stop_decp
