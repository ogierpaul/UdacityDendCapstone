from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import redshiftoperators
import ec2operators


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        redshiftoperators.RedshiftUpsert,
        ec2operators.Ec2Creator,
        ec2operators.Ec2BashExecutor,
        ec2operators.BaseEc2Operator,
        ec2operators.Ec2Terminator
    ]