from airflow.models import Variable


if __name__ == '__main__':
    # Arn role of redshift, needed to access s3
    Variable.set("arn_redshift_s3", 'arn:aws:iam::075227836161:role/redshiftwiths3')
    # Redshift conn id inside of Airflow
    Variable.set("redshift_conn_id", 'aa_redshift')
    # Aws conn id inside of Airflow
    Variable.set("aws_conn_id", 'aws_credentials')
    # S3 bucket for project
    Variable.set("s3_bucket", "paulogiereucentral1")
    # Url of data sources and parameters
    Variable.set("decp_url", 'https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2')
    Variable.set("siren_url", 'https://www.data.gouv.fr/en/datasets/r/573067d3-f43d-4634-9664-675277b81857')
    Variable.set("siren_csvname", 'StockUniteLegale_utf8.csv')
    Variable.set("cpv_csvname", 'cpv_2008_ver_2013.csv')
    Variable.set("infogreffe_curl", "https://opendata.datainfogreffe.fr/api/records/1.0/download/?dataset=chiffres-cles-2020&format=csv&fields=siren,millesime_1,date_de_cloture_exercice_1,duree_1,tranche_ca_millesime_1,ca_1,resultat_1,effectif_1,millesime_2,date_de_cloture_exercice_2,duree_2,tranche_ca_millesime_2,ca_2,resultat_2,effectif_2")
    Variable.set("infogreffe_csvname", 'chiffres-cles-2019.csv')
    # Ec2 Machines configuration
    Variable.set("ec2_config_base", {
        'ImageId': 'ami-0de9f803fcac87f46',
        'InstanceType': 't2.medium',
        'KeyName': 'ec2keypairfrankfurt',
        'IamInstanceProfileName': 'myec2ssms3',
        'SecurityGroupId': 'sg-c21b9eb8',
        'start_sleep': 60
    }
                 )
