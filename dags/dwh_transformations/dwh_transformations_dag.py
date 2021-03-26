from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from redshiftoperators import RedshiftOperator
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
    'execution_timeout': timedelta(seconds=300),
    'working_dir': os.path.dirname(os.path.abspath(__file__))
}


with DAG(
    'dwh_transformations',
    default_args=default_args,
    description='Create or Refresh views in the Data Warehouse layer (DWH)',
    schedule_interval=None,
    tags=['dend', 'siren', 'decp', 'infogreffe', 'cpv']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_dwh = DummyOperator(
        task_id='start_dwh'
    )

    stop_dwh = DummyOperator(
        task_id='stop_dwh'
    )

    create_decp_titulaires_dwh = RedshiftOperator(
        task_id='create_refresh_decp_titulaires_dwh',
        sql='1a_standardize_decp_titulaires.sql'
    )

    create_refresh_decp_marches_dwh = RedshiftOperator(
        task_id='create_refresh_decp_marches_dwh',
        sql='1b_standardize_decp_marches.sql'
    )

    create_refresh_siren_dwh = RedshiftOperator(
        task_id='create_refresh_siren_dwh',
        sql='2_standardize_siren.sql'
    )

    create_refresh_infogreffe_dwh = RedshiftOperator(
        task_id='create_refresh_infogreffe_dwh',
        sql='3_standardize_infogreffe.sql'
    )

    create_refresh_cpv_dwh = RedshiftOperator(
        task_id='create_refresh_cpv_dwh',
        sql='4_standardize_cpv.sql'
    )

    start_dwh >> create_decp_titulaires_dwh >> create_refresh_decp_marches_dwh >> create_refresh_siren_dwh >> create_refresh_infogreffe_dwh >> create_refresh_cpv_dwh >> stop_dwh


