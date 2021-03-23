from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from ec2operators import Ec2BashExecutor, Ec2CurlGet, Ec2Creator, Ec2Terminator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from redshiftoperators import RedshiftUpsert, RedshiftCopyFromS3, RedshiftOperator
from airflow.operators.dummy import DummyOperator
import os

ec2_config = {
    'ImageId': 'ami-0de9f803fcac87f46',
    'InstanceType': 't2.medium',
    'KeyName': 'ec2keypairfrankfurt',
    'IamInstanceProfileName': 'myec2ssms3',
    'SecurityGroupId': 'sg-c21b9eb8',
    'start_sleep': 60
}
infogreffe_config = {
    'url': 'https://opendata.datainfogreffe.fr/api/records/1.0/download/',
    'csvname': 'infogreffe_chiffrecles_2019.csv',
    's3path': 's3://paulogiereucentral1/staging/infogreffe_attributes/infogreffe_chiffrecles_2019.csv',
    'arn': 'arn:aws:iam::075227836161:role/redshiftwiths3'
}
api_parameters = {
    "dataset": "chiffres-cles-2019",
    "format":"csv",
    "fields":"denomination,siren,nic,forme_juridique,code_ape,libelle_ape,adresse,code_postal,ville,date_de_cloture_exercice_1,millesime_1,tranche_ca_millesime_1,duree_1,ca_1,resultat_1,effectif_1"
}

dag_folder = '/Users/paulogier/81-GithubPackages/UdacityDendCapstone/dags/infogreffe/'

_docs_md_fp = os.path.join(
        os.path.abspath(dag_folder),
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
    'redshift_conn_id': 'aa_redshift',
    'aws_conn_id': 'aws_credentials',
    'autocommit': True,
    'tag_key': 'Stream',
    'tag_value': 'Infogreffe',
    'execution_timeout': timedelta(seconds=300),
    'arn': 'arn:aws:iam::075227836161:role/redshiftwiths3'
}

with DAG(
    'infogreffe_from_api_to_redshift',
    default_args=default_args,
    description='Download the Infogreffe data from the API to Redshift',
    schedule_interval=None,
    tags=['dend']
) as dag:
    dag.doc_md = open(_docs_md_fp, 'r').read()

    dummy_start = DummyOperator(
        task_id='start_infogreffe'
    )

    dummy_stop = DummyOperator(
        task_id='stop_siren'
    )

    create_ec2_if_not_exists = Ec2Creator(
        task_id='create_ec2_if_not_exists',
        **ec2_config
    )

    prepare_ec2 = Ec2BashExecutor(
        task_id='prepare_ec2',
        sh=os.path.join(dag_folder, '1_prepare_ec2.sh')
    )

    api_get = Ec2CurlGet(
        task_id='api_get',
        url=infogreffe_config['url'],
        filename=f"/home/ec2-user/infogreffe/{infogreffe_config['csvname']}",
        parameters=api_parameters,
        retry=30
    )

    copy_to_s3 = Ec2BashExecutor(
        task_id='copy_to_s3',
        sh=os.path.join(dag_folder, '3_copy_to_s3.sh'),
        parameters=infogreffe_config,
        sleep=10,
        retry=20
    )

    stop_ec2 = Ec2Terminator(
        task_id='stop_ec2',
        terminate='stop'
        # trigger_rule='all_done'
    )

    create_redshift = RedshiftOperator(
        task_id='create_redshift',
        sql=os.path.join(dag_folder, '5_infogreffe_create_redshift.sql'),
        file_directory=dag_folder
    )

    copy_from_s3 = RedshiftCopyFromS3(
        task_id='copy_from_s3',
        table='infogreffe_attributes',
        schema='staging',
        s3path='s3://paulogiereucentral1/staging/infogreffe_attributes/',
        arn=infogreffe_config['arn'],
        format='csv',
        header=True,
        delimiter=';'
    )

    upsert_datalake = RedshiftUpsert(task_id='upsert_datalake',
                                     redshift_conn_id='aa_redshift',
                                     pkey="siren",
                                     query="SELECT * FROM staging.infogreffe_attributes",
                                     table="siren_attributes",
                                     schema="datalake")

    dummy_start>> create_ec2_if_not_exists >> prepare_ec2 >> api_get >> copy_to_s3 >> stop_ec2
    stop_ec2 >> create_redshift >> copy_from_s3 >> upsert_datalake >> dummy_stop
