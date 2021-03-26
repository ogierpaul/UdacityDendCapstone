from airflow.models.baseoperator import BaseOperator
from airflow.configuration import conf
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S
# import psycopg2
# from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)
# import time
import inspect
import os


def read_instructions(s, file_ending=('.sql'), file_split=';', file_comment='-', file_strip='\n ',
                      working_dir=None):
    # Read the instructions and return the instructions cleaned as a tuple
    ## If the instructions end with file_ending, it indicates this is a file, and it will try to open them relative
    # to ref_dir. It will split on the file_split char
    ## Else, if it is a simple string, it will include it as a tuple
    ## Else, it if is a list or a tuple, it will return it as a tuple
    # For each element, it then strips the strings
    ## And split it
    if working_dir is None:
        working_dir = conf.get('core', 'dags_folder')
    commands_unformatted = ()
    if isinstance(s, str):
        ending = '.' + s.split('.')[-1]
        if ending.rstrip(' ') in file_ending:
            fp = os.path.join(os.path.abspath(working_dir), s.rstrip(' '))
            f = open(fp, 'r')
            commands_unformatted = f.read().split(file_split)
            commands_unformatted = tuple(commands_unformatted)
            f.close()
        else:
            commands_unformatted = (s,)
    else:
        if isinstance(s, tuple):
            commands_unformatted = s
        elif isinstance(s, list):
            commands_unformatted = tuple(s)
        else:
            pass
    commands_stripped = map(lambda c: c.strip(file_strip), commands_unformatted)
    commands_stripped = filter(lambda c: len(c) > 0, commands_stripped)
    commands_stripped = filter(lambda c: c[0] != file_comment, commands_stripped)
    commands_stripped = tuple(commands_stripped)
    return commands_stripped


class RedshiftOperator(BaseOperator):
    ui_color = "#ffc6ff"
    template_fields = ('sql',)
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(self, redshift_conn_id, sql, params_sql=None, working_dir=None, *args, **kwargs):
        self.working_dir = working_dir
        self.redshift_conn_id = redshift_conn_id
        super(RedshiftOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.commands_stripped = read_instructions(
            self.sql,
            working_dir=self.working_dir,
            file_ending=('.sql'),
            file_comment='-',
            file_split=';',
            file_strip=' \n'
        )
        self.params_sql = params_sql

    def execute(self, context=None):
        if self.params_sql is not None:
            commands_formatted = [S.SQL(q).format(**self.params_sql) for q in self.commands_stripped]
        else:
            commands_formatted = [S.SQL(q) for q in self.commands_stripped]
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for qf in commands_formatted:
            self.log.info("Executing Query:{}".format(qf.as_string(hook.get_conn())))
            hook.run((qf,))
            pass


class RedshiftCopyFromS3(RedshiftOperator):
    template_fields = ('sql', 'arn', 's3_bucket', 's3_folder' )
    template_ext = ('.sql', )
    ui_color = "#f4a261"
    q_truncate = "TRUNCATE {schema}.{table};"
    q_copy = """
    COPY {schema}.{table}
    FROM {s3path}
    IAM_ROLE AS {arn}
    REGION {region}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """

    @apply_defaults
    def __init__(self, redshift_conn_id, arn, s3_bucket, s3_folder, fn, schema, table, region_name='eu-central-1',
                 truncate=True, format='csv', delimiter=',', jsonpath='auto', header=True, fillrecord=False, *args, **kwargs):
        option_line = []
        self.arn = arn
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        if format == 'csv':
            option_line.append('FORMAT AS CSV')
            if header is True:
                option_line.append('IGNOREHEADER AS 1')
            option_line.append("DELIMITER AS '{}'".format(delimiter[0]))
            if fillrecord is True:
                option_line.append('FILLRECORD')
        elif format == 'json':
            option_line.append("FORMAT AS JSON '{}'".format(jsonpath))
        option_line = ' '.join(option_line)
        q_copy_with_options = self.q_copy + '\n' + option_line + ';'
        if truncate is True:
            sql = (self.q_truncate, q_copy_with_options)
        else:
            sql = (q_copy_with_options,)
        s3path = 's3://' + self.s3_bucket.rstrip('/') + '/' + self.s3_folder.rstrip('/') + '/'
        if fn is not None:
            s3path = s3path + fn
        params_sql = {
            'arn': S.Literal(self.arn),
            'schema': S.Identifier(schema),
            'table': S.Identifier(table),
            's3path': S.Literal(s3path),
            'region': S.Literal(region_name),
            'jsonpath': S.Literal(jsonpath)
        }
        super(RedshiftCopyFromS3, self).__init__(redshift_conn_id=redshift_conn_id, sql=sql, params_sql=params_sql,
                                                 *args, **kwargs)
        pass


class RedshiftUpsert(RedshiftOperator):
    ui_color = '#caffbf'
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
                 sql="",
                 schema="",
                 table="",
                 pkey="",
                 stageprefix="stageupsert_",
                 working_dir=None,
                 *args, **kwargs):
        """

        Args:
            redshift_conn_id (str): in Airflow Connection Database, name of Redshift connection
            pkey (str): primary key of the table (should be one column
            sql (str): query to execute which returns values to be upserted (SELECT FROM without ;). Values sould be distnct, no duplicates
            stageprefix (str): prefix to be added for a temporary staging table to allow upsert
            table (str): Target table to Upsert
            *args:
            **kwargs:
        """
        query_select = read_instructions(s=sql, working_dir=working_dir)[0].rstrip(';')
        params_sql = {
            'pkey': S.Identifier(pkey),
            'table': S.Identifier(table),
            'schema': S.Identifier(schema),
            'stage': S.Identifier("".join([stageprefix, table])),
            'query': S.SQL(query_select)
        }
        super(RedshiftUpsert, self).__init__(redshift_conn_id=redshift_conn_id, sql=self.q_all, params_sql=params_sql,
                                             *args, **kwargs)
