from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
from botocore.exceptions import ClientError


class S3UploadFromLocal(BaseOperator):
    ui_color = "#9bf6ff"

    @apply_defaults
    def __init__(self, aws_conn_id, fp, s3_bucket, s3_key, *args, **kwargs):
        super(S3UploadFromLocal, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.aws_hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type='s3')
        self.region_name = 'eu-central-1'
        self.aws_credentials = self.aws_hook.get_credentials()
        self.aws_access_key_id = self.aws_credentials.access_key
        self.aws_secret_access_key = self.aws_credentials.secret_key
        self.s3client = boto3.client('s3',
                                     region_name=self.region_name,
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.fp = fp

    def execute(self, context):
        try:
            response = self.s3client.upload_file(self.fp, self.s3_bucket, self.s3_key)
            self.log.info(f'{self.fp} uploaded to s3://{self.s3_bucket}/{self.s3_key}')
        except ClientError as e:
            self.log.error(e)
            raise ClientError(e)
        pass


