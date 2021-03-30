from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from redshiftoperators import RedshiftOperator, RedshiftQualityCheck

import os

default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'redshift_conn_id': 'aa_redshift',
    'autocommit': True,
    'execution_timeout': timedelta(seconds=300),
    'working_dir': os.path.dirname(os.path.abspath(__file__))
}

with DAG(
    'siren_dwh_transformations',
    default_args=default_args,
    description='Refresh siren data in the Data Warehouse layer (DWH)',
    schedule_interval=None,
    tags=['dend', 'siren', 'dwh']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_refresh = DummyOperator(
        task_id='start_refresh'
    )

    create_dwh = RedshiftOperator(
        task_id='create_siren_dwh',
        sql='schema_siren_dwh.sql'
    )
    refresh_dwh = RedshiftOperator(
        task_id='refresh_siren_dwh',
        sql='refresh_siren.sql'
    )

    q_check = RedshiftQualityCheck(
        task_id='quality_check',
        schema="dwh",
        table="siren_attributes",
        pkey="siren"
    )

    stop_refresh = DummyOperator(
        task_id='stop_refresh'
    )

    start_refresh >> create_dwh >> refresh_dwh >> q_check >> stop_refresh
