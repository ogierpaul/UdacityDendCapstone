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
    'tag_value': 'Decp',
    'execution_timeout': timedelta(seconds=300),
}
ec2_config = {
    'ImageId': 'ami-0de9f803fcac87f46',
    'InstanceType': 't2.medium',
    'KeyName': 'ec2keypairfrankfurt',
    'IamInstanceProfileName': 'myec2ssms3',
    'SecurityGroupId': 'sg-c21b9eb8',
    'StartSleep': 60
}
script_folder = '/usr/local/airflow/dags/decp/1_download_from_web/'

decp_config = {
    'bucket': 'paulogiereucentral1',
    'url': 'https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2'
}

dag = DAG(
    'decp_test_dag',
    default_args=default_args,
    description='Decp Dag',
    schedule_interval=None
)



upload_config_marches = S3UploadFromLocal(
    task_id='Upload_config_marches',
    dag=dag,
    fp=script_folder + 'jq_marches.sh',
    s3_bucket=decp_config['bucket'],
    s3_key='config/jq_marches.sh'
)

upload_config_titulaires = S3UploadFromLocal(
    task_id='Upload_config_titulaires',
    dag=dag,
    fp=script_folder + 'jq_titulaires.sh',
    s3_bucket=decp_config['bucket'],
    s3_key='config/jq_titulaires.sh'
)

create_ec2 = Ec2Creator(
    task_id = 'decp_create_ec2',
    dag=dag,
    retry=20,
    sleep=5,
    **ec2_config
)

prepare_ec2 = Ec2BashExecutor(
    task_id='decp_prepare_ec2',
    dag=dag,
    sh=script_folder + '1_prepare_ec2.sh',
    parameters=decp_config
)

download_from_web = Ec2BashExecutor(
    task_id='decp_download_from_web',
    dag=dag,
    sh=script_folder + '2_download_from_web.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

create_json_temp = Ec2BashExecutor(
    task_id='decp_create_json_temp',
    dag=dag,
    sh=script_folder + '3_create_jq_temp.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

extract_marches = Ec2BashExecutor(
    task_id='decp_extract_marches',
    dag=dag,
    sh=script_folder + '4_extract_marches.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

extract_titulaires = Ec2BashExecutor(
    task_id='decp_extract_titulaires',
    dag=dag,
    sh=script_folder + '4_extract_titulaires.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

copy_titulaires_s3 = Ec2BashExecutor(
    task_id='copy_titulaires_s3',
    dag=dag,
    sh=script_folder + '5_copy_titulaires.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

copy_marches_s3 = Ec2BashExecutor(
    task_id='copy_marches_s3',
    dag=dag,
    sh=script_folder + '5_copy_marches.sh',
    parameters=decp_config,
    retry=20,
    sleep=5
)

clean_ec2 = Ec2BashExecutor(
    task_id='clean_ec2',
    dag=dag,
    sh=script_folder + '6_clean_ec2.sh',
    parameters=decp_config
)

# stop_ec2 = Ec2Terminator(
#     task_id = 'stop_ec2',
#     dag=dag,
#     terminate='stop'
# )

upload_config_marches >> create_ec2
upload_config_titulaires >> create_ec2
create_ec2 >> prepare_ec2 >> download_from_web >> create_json_temp
create_json_temp >> extract_marches >> copy_marches_s3
create_json_temp >> extract_titulaires >> copy_titulaires_s3
# [copy_marches_s3, copy_titulaires_s3] >>  clean_ec2 >> stop_ec2

