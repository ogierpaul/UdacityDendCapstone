from airflow.models import BaseOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S
from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)
import time


def read_format_sql(fp, **params):
    """
    Read a file containing SQL statements separated by ;
    Format them using psycopg2 SQL module and the params arguments
    Args:
        fp (str): file path
        **params: SQL.Identifier, SQL.Literral, or else

    Returns:
        list
    """
    with open(fp, 'r') as f:
        statements = f.read()
    f.close()
    sql_all = [S.SQL(q.strip() + ';').format(**params) for q in statements.split(';') if q.strip() != '']
    return sql_all


class RedshiftUpsert(PostgresOperator):
    """
    Demonstrator of an Operator to UPSERT data in Redshift, and then perform data quality checks
    See pseudocode here: https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
    Upsert:
    1. Create  an empty Temporary Staging Table and fill it with the values to be inserted in the Target Table
    2. Delete rows from Target table that are present in Staging tables
    3. Insert into Target Tables from Staging Table
    4. Delete Staging Table

    Props:
    - q_all: list of all queries that will be executed for upsert
    - qf_ stands for a template query q_ formatted with arguments
    - params: dictionnary of parameters: table name, staging table name, primary key, query used to select

    Notes:
        - Select Distinct On is not supported by Redshift
        - See here a workaround : https://gist.github.com/jmindek/62c50dd766556b7b16d6
        - Make sure that there are no duplicates in the select query
    """
    ui_color = '#caffbf'
    # Queries used for Upsert
    q_temp_drop = """DROP TABLE IF EXISTS {schema}.{stage};"""
    q_temp_create = """CREATE TABLE IF NOT EXISTS {schema}.{stage}  (LIKE {schema}.{table});"""
    q_temp_load = """
    INSERT INTO {schema}.{stage} {query};
    """
    q_begin_transaction = """BEGIN TRANSACTION;"""
    q_delete_target = """DELETE FROM {schema}.{table} USING {schema}.{stage} WHERE {schema}.{table}.{pkey} = {schema}.{stage}.{pkey};"""
    q_insert_target = """INSERT INTO {schema}.{table} SELECT DISTINCT * FROM {schema}.{stage};"""
    q_end_transaction = """END TRANSACTION;"""
    q_all = [q_temp_drop, q_temp_create, q_temp_load, q_begin_transaction, q_delete_target, q_insert_target,
             q_end_transaction, q_temp_drop]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 pkey="",
                 query="",
                 stageprefix="stageupsert_",
                 schema="",
                 table="",
                 *args, **kwargs):
        """

        Args:
            redshift_conn_id (str): in Airflow Connection Database, name of Redshift connection
            pkey (str): primary key of the table (should be one column
            query (str): query to execute which returns values to be upserted (SELECT FROM without ;)
            stageprefix (str): prefix to be added for a temporary staging table to allow upsert
            table (str): Target table to Upsert
            *args:
            **kwargs:
        """
        params = {
            'pkey': S.Identifier(pkey),
            'table': S.Identifier(table),
            'schema':S.Identifier(schema),
            'stage': S.Identifier("".join([stageprefix, table])),
            'query': S.SQL(query.strip(';'))
        }
        qf_all = [S.SQL(q).format(**params) for q in RedshiftUpsert.q_all]
        super(RedshiftUpsert, self).__init__(postgres_conn_id=redshift_conn_id, sql=qf_all, **kwargs)



