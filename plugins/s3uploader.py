from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
from botocore.exceptions import ClientError
import os

class S3UploadFromLocal(BaseOperator):
    ui_color = "#e76f51",
    template_fields = ('working_dir', 'fn', 's3_bucket',  's3_folder',)

    @apply_defaults
    def __init__(self, aws_conn_id, working_dir, fn, s3_bucket, s3_folder, region_name='eu-central-1', *args, **kwargs):
        """
        Upload a local file to S3
        Args:
            aws_conn_id (str): Aws Conneciton Id in Airflow
            working_dir (str): directory where the file is located
            fn (str): Filename
            s3_bucket (str): bucket where to upload the file
            s3_folder (str): folder where to upload the file
            region_name (str): region name of S3
            *args:
            **kwargs:
        """
        super(S3UploadFromLocal, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.working_dir = working_dir
        self.fn = fn

    def execute(self, context):
        """
        Upload the file using boto3 S3 client
        Args:
            context:

        Returns:

        """
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type='s3')
        aws_credentials = aws_hook.get_credentials()
        aws_access_key_id = aws_credentials.access_key
        aws_secret_access_key = aws_credentials.secret_key
        s3client = boto3.client('s3',
                                     region_name=self.region_name,
                                     aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
        try:
            self.fp = os.path.join(self.working_dir, self.fn)
            s3_key = self.s3_folder + self.fn
            s3_path = 's3://' + self.s3_bucket.rstrip('/') +'/' + s3_key
            self.log.info(f'uploading {self.fp}  to {s3_path}')
            response = s3client.upload_file(self.fp, self.s3_bucket, s3_key)
            self.log.info(response)
        except ClientError as e:
            self.log.error(e)
            raise ClientError(e)
        pass


