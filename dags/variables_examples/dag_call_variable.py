from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'redshift_conn_id': 'aa_redshift',
    'aws_conn_id': 'aws_credentials',
    'autocommit': True,
    'tag_key': 'Stream',
    'tag_value': 'Infogreffe',
    'execution_timeout': timedelta(seconds=300)
}


with DAG(
    'aa_test_variable',
    default_args=default_args,
    description='Show Variables usedt',
    schedule_interval=None,
    tags=['test']
) as dag:
    echo_execution_date = BashOperator(
        task_id='echo_execution',
        bash_command="echo {{ execution_date }}",
    )
    echo_variable = BashOperator(
        task_id='echo_variable',
        bash_command="echo {{ var.value.test_schema_name }}",
    )
    echo_params = BashOperator(
        task_id='echo_params',
        bash_command="echo {{ params.myparam }}",
        params={'myparam': "{{ var.value.test_schema_name }}"}
    )

    echo_script = BashOperator(
        task_id='echo_script',
        bash_command='script_with_variable.sh'
    )

    execute_sql = PostgresOperator(
        sql="""SELECT * FROM {{ var.value.test_schema_name }}.{{ var.value.test_table_name }} LIMIT 5;""",
        postgres_conn_id='aa_redshift',
        task_id='execute_sql',
        autocommit=True
    )

    execute_script_sql = PostgresOperator(
        sql='script_with_variable.sql',
        postgres_conn_id='aa_redshift',
        task_id='execute_script_sql',
        autocommit=True
    )

    echo_execution_date >> echo_variable >> echo_params >> echo_script >> execute_script_sql >> execute_sql