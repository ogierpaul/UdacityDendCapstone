from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import Ec2BashExecutor, BaseEc2Operator, Ec2Creator, Ec2Terminator, S3UploadFromLocal
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators import RedshiftUpsert, RedshiftCopyFromS3, RedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
import os

cpv_config = {
    'csvname': 'cpv_2008_ver_2013.csv',
    's3path': 's3://paulogiereucentral1/staging/cpv_attributes/cpv_2008_ver_2013.csv',
    'arn': 'arn:aws:iam::075227836161:role/redshiftwiths3'
}

dag_folder = '/usr/local/airflow/dags/cpv/'

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
    'cpv_from_local_to_redshift',
    default_args=default_args,
    description='Upload the CPV File from local system to S3 and to Redshift',
    schedule_interval=None
) as dag:
    dag.doc_md = open(_docs_md_fp, 'r').read()

    # start_cpv = DummyOperator(
    #     task_id='start_cpv',
    #     dag=dag
    # )
    #
    # stop_cpv = DummyOperator(
    #     task_id='stop_cpv',
    #     dag=dag
    # )
    #
    # upload_cpv_to_s3 = S3UploadFromLocal(
    #     task_id='Upload_config_titulaires',
    #     dag=dag,
    #     fp=dag_folder + cpv_config['csvname'],
    #     s3_bucket=cpv_config['s3_bucket'],
    #     s3_key=cpv_config['s3_key'] + '/cpv_attributes.csv',
    # )

    create_redshift = RedshiftOperator(
        task_id='create_redshift',
        dag=dag,
        sql='0_create_schema_redshift.sql'
    )


    copy_from_s3 = RedshiftCopyFromS3(
        task_id='copy_from_s3',
        dag=dag,

        schema='staging',
        table='cpv_attributes',
        s3path=cpv_config['s3path'],
        csv=True,
        header=True,
        truncate=True,
        delimiter = '|',
        arn=cpv_config['arn']
    )

    upsert_datalake = RedshiftUpsert(task_id='upsert_datalake',
                                     dag=dag,
                                     pkey="cpv",
                                     query="SELECT * FROM staging.cpv_attributes;",
                                     table="cpv_attributes",
                                     schema="datalake")

    create_redshift >>  copy_from_s3 >> upsert_datalake
