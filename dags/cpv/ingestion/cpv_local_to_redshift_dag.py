from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
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
    'execution_timeout': timedelta(seconds=300),
    's3_bucket': Variable.get('s3_bucket'),
    'arn': Variable.get('arn'),
    'fn': Variable.get('cpv_csvname'),
    'working_dir': os.path.dirname(os.path.abspath(__file__))
}


with DAG(
    'cpv_from_local_to_redshift',
    default_args=default_args,
    description='Upload the CPV File from local system to S3 and to Redshift',
    schedule_interval=None,
    tags=['dend', 'cpv', 'staging']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_cpv = DummyOperator(
        task_id='start_cpv'
    )

    stop_cpv = DummyOperator(
        task_id='stop_cpv'
    )

    upload_cpv_to_s3 = S3UploadFromLocal(
        task_id='Upload_cpv_to_s3s',
        s3_folder='staging/cpv_attributes/'
    )

    create_redshift = RedshiftOperator(
        task_id='create_redshift',
        sql='schema_cpv_staging_datalake.sql'
    )

    copy_from_s3 = RedshiftCopyFromS3(
        task_id='copy_from_s3',
        s3_folder='staging/cpv_attributes',
        schema='staging',
        table='cpv_attributes',
        format='csv',
        header=True,
        delimiter='|'
    )

    upsert_datalake = RedshiftUpsert(
         task_id='upsert_datalake',
         schema="datalake",
         table="cpv_attributes",
         pkey="codecpv",
         sql="SELECT * FROM staging.cpv_attributes"
    )

    q_check = RedshiftQualityCheck(
        task_id='quality_check',
        schema="datalake",
        table="cpv_attributes",
        pkey="codecpv"
    )

    start_cpv >> upload_cpv_to_s3 >> create_redshift >>  copy_from_s3 >> upsert_datalake >> q_check >> stop_cpv
