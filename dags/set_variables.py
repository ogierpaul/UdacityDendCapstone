from airflow.models import Variable


if __name__ == '__main__':
    Variable.set("arn", 'arn:aws:iam::075227836161:role/redshiftwiths3')
    Variable.set("redshift_conn_id", 'aa_redshift')
    Variable.set("aws_conn_id", 'aws_credentials')
    Variable.set("tag_key", "Capstone_Stream")
    Variable.set("s3_bucket", "paulogiereucentral1")
    Variable.set("tag_value_infogreffe", "infogreffe")
    Variable.set("tag_value_siren", "siren")
    Variable.set("tag_value_cpv", "cpv")
    Variable.set("tag_value_decp", "decp")
    Variable.set("tag_value_dec", "decp")
    Variable.set("ec2_config_base", {
        'ImageId': 'ami-0de9f803fcac87f46',
        'InstanceType': 't2.medium',
        'KeyName': 'ec2keypairfrankfurt',
        'IamInstanceProfileName': 'myec2ssms3',
        'SecurityGroupId': 'sg-c21b9eb8',
        'start_sleep': 60
    }
                 )
    Variable.set(
        "config_siren", {
            'url': 'https://www.data.gouv.fr/en/datasets/r/573067d3-f43d-4634-9664-675277b81857',
            'filename': 'StockUniteLegale_utf8.csv',
            's3_key': 'staging/siren_attributes/'
        }
    )
    Variable.set(
        "config_cpv", {
            'filename': 'cpv_2008_ver_2013.csv',
            's3_key': 'staging/siren_attributes'
        }
    )
    Variable.set(
        "config_infogreffe", {
            'url': 'https://opendata.datainfogreffe.fr/api/records/1.0/download/',
            'filename': 'infogreffe_chiffrecles_2019.csv',
            's3_key': 'staging/siren_attributes'}
    )
    Variable.set("config_decp", {
        "url": 'https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2',
        "filename_marches": 'decp_marches.csv',
        "filename_titulaires": 'decp_titulaires.csv',
        "s3_key_marches": 'staging/decp_marches',
        "s3_key_titulaires": 'staging/decp_titulaires'
    })
    Variable.set("test_schema_name", "staging")
    Variable.set("test_table_name", "cpv_attributes")
