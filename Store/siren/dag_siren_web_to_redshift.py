from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import Ec2BashExecutor, BaseEc2Operator, Ec2Creator, Ec2Terminator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators import RedshiftUpsert, RedshiftCopyFromS3, RedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
import os

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
    'output_s3': 's3://paulogiereucentral1/staging/siren_attributes/StockUniteLegale_utf8.csv',
    'arn': 'arn:aws:iam::075227836161:role/redshiftwiths3'
}

dag_folder = '/usr/local/airflow/dags/siren/'

_docs_md_fp = os.path.join(
        os.path.dirname(dag_folder),
        'Readme.md'
    )
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

with DAG(
    'siren_from_web_to_redshift',
    default_args=default_args,
    description='Download the Siren File from the web to Redshift',
    schedule_interval=None
) as dag:
    dag.doc_md = open(_docs_md_fp, 'r').read()

    # start_siren = DummyOperator(
    #     task_id='start_siren',
    #     dag=dag
    # )
    #
    # stop_siren = DummyOperator(
    #     task_id='stop_siren',
    #     dag=dag,
    #     trigger_rule='all_done'
    # )
    #
    # create_ec2_if_not_exists = Ec2Creator(
    #     task_id='create_ec2_if_not_exists',
    #     dag=dag,
    #     **ec2_config
    # )
    #
    # prepare_ec2 = Ec2BashExecutor(
    #     task_id='prepare_ec2',
    #     dag=dag,
    #     sh=os.path.join(dag_folder, '1_siren_ec2_init.sh')
    # )
    #
    # download_from_web = Ec2BashExecutor(
    #     task_id='download_from_web',
    #     dag=dag,
    #     sh=os.path.join(dag_folder, '2_siren_web_download.sh'),
    #     parameters=siren_config,
    #     sleep=5,
    #     retry=20
    # )

    # copy_to_s3 = Ec2BashExecutor(
    #     task_id='copy_to_s3',
    #     dag=dag,
    #     sh=os.path.join(dag_folder, '3_copy_to_s3.sh'),
    #     parameters=siren_config,
    #     sleep=10,
    #     retry=20
    # )
    #
    # stop_ec2 = Ec2Terminator(
    #     task_id='stop_ec2',
    #     dag=dag,
    #     terminate='stop',
    #     trigger_rule='all_done'
    # )

    create_redshift = RedshiftOperator(
        task_id='create_redshift',
        dag=dag,
        sql='4_create_redshift.sql'
    )

    copy_from_s3 = RedshiftCopyFromS3(
        task_id='truncate_staging',
        dag=dag,
        s3path=siren_config['output_s3'],
        arn=siren_config['arn'],
        csv=True,
        header=True,
        delimiter=','
    )

    upsert_datalake = RedshiftUpsert(task_id='upsert_datalake',
                                     dag=dag,
                                     redshift_conn_id='aa_redshift',
                                     pkey="siren",
                                     query="SELECT * FROM staging.siren_attributes",
                                     table="siren_attributes",
                                     schema="datalake")

    # start_siren >> create_ec2_if_not_exists >> prepare_ec2 >> download_from_web >> copy_to_s3 >> stop_ec2
    create_redshift >> copy_from_s3 >> upsert_datalake
