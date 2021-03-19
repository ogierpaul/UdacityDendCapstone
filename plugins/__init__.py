from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import redshiftoperators
import ec2operators
import s3uploader


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        redshiftoperators.RedshiftUpsert,
        redshiftoperators.RedshiftCopyFromS3,
        redshiftoperators.RedshiftOperator,
        ec2operators.Ec2Creator,
        ec2operators.Ec2BashExecutor,
        ec2operators.BaseEc2Operator,
        ec2operators.Ec2Terminator,
        s3uploader.S3UploadFromLocal
    ]