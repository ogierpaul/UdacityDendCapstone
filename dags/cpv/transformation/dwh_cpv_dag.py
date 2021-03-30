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
    'cpv_dwh_transformations',
    default_args=default_args,
    description='Refresh CPV data in the Data Warehouse layer (DWH)',
    schedule_interval=None,
    tags=['dend', 'cpv', 'dwh']
) as dag:
    _docs_md_fp = os.path.join(default_args['working_dir'], 'Readme.md')
    dag.doc_md = open(_docs_md_fp, 'r').read()

    start_refresh = DummyOperator(
        task_id='start_refresh'
    )

    create_cpv_dwh = RedshiftOperator(
        task_id='create_cpv_dwh',
        sql='schema_cpv_dwh.sql'
    )
    refresh_cpv_dwh = RedshiftOperator(
        task_id='refresh_cpv_dwh',
        sql='refresh_cpv.sql'
    )

    q_check = RedshiftQualityCheck(
        task_id='quality_check',
        schema="dwh",
        table="cpv_attributes",
        pkey="codecpv"
    )

    stop_refresh = DummyOperator(
        task_id='stop_refresh'
    )

    start_refresh >> create_cpv_dwh >> refresh_cpv_dwh >> q_check >> stop_refresh
