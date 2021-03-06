import boto3
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.configuration import conf
import time
import os

class BaseEc2Operator(BaseOperator):
    template_fields = ('tag_key', 'tag_value')

    @apply_defaults
    def __init__(self, aws_conn_id, tag_key, tag_value, retry=10, sleep=10, region_name='eu-central-1', *args,
                 **kwargs):
        """
        This Operator interacts with EC2.
        Using AwsHook, it can instantiate a boto3 ressource or client to interact with Ec2 instances.
        Ec2 machines are recognized through a (tag_key, tag_value) combination.
        It thus allow to have multipl operators working on the same instance, even if the InstanceId changes at each DAG run.

        :param aws_conn_id-->str: The AwsHook string name in Airflow
        :param tag_key -->str: A tag key. In combination (tag_key, tag_value), identifies an EC2 Instance.
        :param tag_value: A tag value. In combination (tag_key, tag_value), identifies an EC2 Instance.
        :param retry-->str:
        :param sleep: Sleep time before testing again status update.
        :param region_name: Aws Region Name of Ec2 Instance
        :param args:
        :param kwargs:
        """
        super(BaseEc2Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.tag_key = tag_key
        self.tag_value = tag_value
        self.retry = retry
        self.sleep = sleep

    def _get_hook(self):
        """

        :return: AwsBaseHook from airflow.providers.amazon.aws.hooks.base_aws
        """
        return AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type='ec2')

    def _get_ecc(self):
        """

        :return: boto3.client instance for Ec2 service.
        """
        aws_hook = self._get_hook()
        aws_credentials = aws_hook.get_credentials()
        aws_access_key_id = aws_credentials.access_key
        aws_secret_access_key = aws_credentials.secret_key
        ecc = boto3.client('ec2',
                           region_name=self.region_name,
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key
                           )
        return ecc

    def _get_ecr(self):
        """

        :return: boto3.ressource instance for Ec2 service.
        """
        aws_hook = self._get_hook()
        aws_credentials = aws_hook.get_credentials()
        aws_access_key_id = aws_credentials.access_key
        aws_secret_access_key = aws_credentials.secret_key
        ecr = boto3.resource('ec2',
                             region_name=self.region_name,
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key
                             )
        return ecr

    def _filter_per_tag(self):
        """
        Filter instance per tag
        Args:

        Returns:
            list: list of strings , instances ids
        """

        query = [{
            "Name": f"tag:{self.tag_key}",
            "Values": [self.tag_value]
        }]
        ecc = self._get_ecc()
        props = ecc.describe_instances(Filters=query)['Reservations']
        if len(props) > 0:
            res = [i['Instances'][0]['InstanceId'] for i in props]
        else:
            res = []
        return res

    def _get_instance_status(self, Instanceid):
        """
        Return a simplified instance status: 'available', 'stopped', 'deleting', 'modifying', None
        Used to smoothen the instance check before launching any queries
        Args:
            Instanceid (str): Instance number

        Returns:

        """
        assert isinstance(Instanceid, str)
        ecc = self._get_ecc()
        out = ecc.describe_instance_status(InstanceIds=[Instanceid], IncludeAllInstances=True)
        props = out['InstanceStatuses'][0]
        instance_status = props['InstanceStatus']['Status']
        system_status = props['SystemStatus']['Status']
        instance_state = props['InstanceState']['Name']
        if system_status == 'ok' and instance_status == 'ok' and instance_state == 'running':
            return 'available'
        elif instance_state in ['stopping', 'stopped']:
            return 'stopped'
        elif instance_state in ['shutting-down', 'terminated']:
            return 'deleting'
        elif instance_state in ['pending', 'resizing'] \
            or system_status in ['initializing'] \
            or instance_status in ['initializing']:
            return 'modifying'
        else:
            return None

    def execute(self, context):
        pass

    def _filter_per_tag_per_status(self, state='available'):
        """
        Filter on custom states the EC2 machines matching the TAG_KEY, TAG_VALUE
        Returns
            list: list of Instance Ids
        """
        assert state in ['available', 'modifying', 'stopped']
        instances = self._filter_per_tag()
        instances_status = [(c_id, self._get_instance_status(c_id)) for c_id in instances]
        target_instances = [c_id for c_id, c_stat in instances_status if c_stat == state]
        return target_instances


class Ec2Creator(BaseEc2Operator):
    """
    This operator search for an available instance matching the (tag_key, tag_value) parameter.
    It either:
    - Finds an available one --> do nothing
    - Finds a stopped one --> start
    - Finds a modifying one --> probe regularly for status until it is available or the max number retry have been reached
    - Finds none --> Create it
    """
    ui_color = "#ffadad"

    @apply_defaults
    def __init__(self, aws_conn_id, tag_key, tag_value, ImageId, KeyName, InstanceType,
                 SecurityGroupId, IamInstanceProfileName, retry=10, sleep=20, start_sleep=60, *args, **kwargs):
        """
        Args:
            ImageId (str): VM (AMI) image Id
            KeyName (str): PEM key name
            InstanceType (str): instance type , ex t2.micro
            SecurityGroupId (str): Security Group Name
            IamInstanceProfileName (str): Iam Role Name
            TAG_KEY (str): Tag Key
            TAG_VALUE (str): Tag Value,
            retry (int): number of times the operator will check for status until it times out
            sleep (int): number of seconds between each status proble
            start_sleep (int): number of seconds to wait if the machine has been created

        """
        super(Ec2Creator, self).__init__(aws_conn_id=aws_conn_id, tag_key=tag_key, tag_value=tag_value, retry=retry,
                                         sleep=sleep, *args, **kwargs)
        self.ImageId = ImageId
        self.KeyName = KeyName
        self.InstanceType = InstanceType
        self.SecurityGroupId = SecurityGroupId
        self.IamInstanceProfileName = IamInstanceProfileName
        self.start_sleep = start_sleep

    def _create_instance(self):
        """

        Returns:
            boto3.instance
        """
        ecr = self._get_ecr()
        new_instance = ecr.create_instances(
            ImageId=self.ImageId,
            KeyName=self.KeyName,
            InstanceType=self.InstanceType,
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.SecurityGroupId],
            IamInstanceProfile={
                'Name': self.IamInstanceProfileName
            },
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{
                    "Key": self.tag_key,
                    "Value": self.tag_value
                }]
            }]
        )

        return new_instance

    def _get_or_create(self):
        """
        This operator search for an available instance matching the (tag_key, tag_value) parameter.
        It either:
        - Finds an available one --> do nothing
        - Finds a stopped one --> start
        - Finds a modifying one --> probe regularly for status until it is available or the max number retry have been reached
        - Finds none --> Create it
        :return: id of the instance available
        """
        n = 0
        res_id = None
        # Loop (retry) times while waiting (sleep) secondes between each loop
        # Loop until either max number of tries has been done, or an available instance has been found
        ecr = self._get_ecr()
        while n <= self.retry and res_id is None:
            n += 1
            # 2. Get the list of instances matching the tag_key and tag_value
            instances = self._filter_per_tag()

            # 2.1. Case none exists
            if instances is None or len(instances) == 0:
                available_instances = stopped_instances = pending_instances = []
            # 2.2. Case some exist, find available, stopped, or pending instances
            else:
                instances_status = [(c_id, self._get_instance_status(c_id)) for c_id in instances]
                instances_as_string = '\n'.join([', '.join(c) for c in instances_status])
                self.log.info(f"Try {n} of {self.retry} Instances:\n{instances_as_string}")
                available_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'available']
                stopped_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'stopped']
                pending_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'modifying']

            # 3. Decide what to do base on status of matching instances
            # 3.1. If there are some available instances>: get their id (this will stop the loop)
            if len(available_instances) > 0:
                res_id = available_instances[0][0]
                self.log.info(f"available instance {res_id}")
            # 3.2. If there is a stopped instance, restart it, and wait (sleep) seconds, this will become available in next loop
            elif len(stopped_instances) > 0:
                i_id = stopped_instances[0][0]
                ecr.instances.filter(InstanceIds=[i_id]).start()
                self.log.info(f"restart instance {i_id}")
                time.sleep(self.start_sleep)
            # 3.3. If there is a pending (rebooting, modifying...) instance: wait (sleep) secondes
            elif len(pending_instances) > 0:
                time.sleep(self.sleep)
            # 3.4. Last case. If there is no available, stopped or pending instance: start one.
            else:
                i_id = self._create_instance()[0].id
                self.log.info(f"New instance created {i_id}")
                time.sleep(self.start_sleep)

        # 4.: After either all the loops have been run, or an available instance has been found and its id filled:
        if res_id is None:
            raise ConnectionError(f'Unable to create instance with tag ({self.tag_key}, {self.tag_value})')
        else:
            return res_id

    def execute(self, context):
        self.log.info(f"TAG_KEY {self.tag_key} TAG_VALUE {self.tag_value}")
        self._get_or_create()
        self.log.info("END OF EXECUTION")
        pass


class Ec2BashExecutor(BaseEc2Operator):
    ui_color = "#ffd6a5"
    template_fields = ('sh', 'tag_key', 'tag_value', 'params')
    template_ext = ('.sh', )

    @apply_defaults
    def __init__(self, aws_conn_id, tag_key, tag_value, bash, working_dir=None, retry=10, sleep=3, *args, **kwargs):
        """
        Run commands on an ec2 instance
        Args:
            aws_conn_id: Airflow connection string
            tag_key: Tag Key
            tag_value: Tag Value, identifies the EC2 instance
            bash (str): bash command to run, can be a file or a string
            working_dir (str): Working directory where the file is located
            retry: Number of time the status of the command is probed
            sleep: Interval between each probe
            *args:
            **kwargs:
        """
        super(Ec2BashExecutor, self).__init__(
            aws_conn_id=aws_conn_id, tag_key=tag_key, tag_value=tag_value, retry=retry, sleep=sleep, *args, **kwargs
        )
        if working_dir is None:
            self.working_dir = os.path.abspath(conf.get('CORE', 'dags_folder'))
        else:
            self.working_dir = os.path.abspath(working_dir)
        self.sh = self._read_commands(input=bash, folder=working_dir)

    def _get_ssm(self):
        aws_hook = self._get_hook()
        aws_credentials = aws_hook.get_credentials()
        aws_access_key_id = aws_credentials.access_key
        aws_secret_access_key = aws_credentials.secret_key
        ssm = boto3.client('ssm',
                           region_name=self.region_name,
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key
                           )
        return ssm

    def _read_commands(self, input, folder=None):
        """

        :param input:
        :return: list
        """
        commands_unformatted = []
        if isinstance(input, str):
            if input.endswith('.sh'):
                fp = os.path.join(self.working_dir, input)
                f = open(fp, 'r')
                commands_unformatted = f.read().split('\n')
                f.close()
            else:
                commands_unformatted = [input]
        else:
            if isinstance(input, list) or isinstance(input, tuple):
                commands_unformatted = input
            else:
                pass
        commands_stripped = map(lambda c: c.strip(), commands_unformatted)
        commands_stripped = filter(lambda c: len(c) > 0, commands_stripped)
        commands_stripped = filter(lambda c: c[0] != '#', commands_stripped)
        commands_stripped = list(commands_stripped)
        return commands_stripped

    def _send_single_command(self, q, InstanceId):
        ssm = self._get_ssm()
        response = ssm.send_command(
            InstanceIds=[InstanceId],
            DocumentName='AWS-RunShellScript',
            Parameters={"commands": [q]}
        )
        command_id = response['Command']['CommandId']
        time.sleep(3)
        n = 0
        finished = False
        output = None
        status = "Waiting Status"
        while n < self.retry and finished is False:
            n += 1
            output = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=InstanceId
            )
            status = output['Status']
            self.log.info(f"Try {n} of {self.retry}: status {status}")
            if status == 'Success':
                finished = True
                self.log.info(f"StandardOutputContent: {output['StandardOutputContent']}")
                break
            elif status in ['Pending', 'Delayed', 'InProgress', 'Cancelling']:
                finished = False
                time.sleep(self.sleep)
            elif status in ['Cancelled', 'TimedOut', 'Failed']:
                finished = True
                self.log.warning("Failed command")
                self.log.info(f"StandardOutputContent: {output['StandardOutputContent']}")
                self.log.info(f"StandardErrorContent: {output['StandardErrorContent']}")
                break
            else:
                pass
        return status

    def execute(self, context):
        self.log.info("Starting Execution of Ec2 BashOperator")
        self.log.info(f"TAG_KEY {self.tag_key} TAG_VALUE {self.tag_value}")
        if len(self._filter_per_tag_per_status(state='available')) == 0:
            self.log.error(f"No instance available")
            raise ConnectionError("No instance available")
        else:
            InstanceId = self._filter_per_tag_per_status(state='available')[0]
            assert isinstance(InstanceId, str)
            for q in self.sh:
                self.log.info(f"Sending command:\n{q}")
                o = self._send_single_command(q, InstanceId)
                if o != 'Success':
                    self.log.error(f"Command failed status:{o}")
                    raise ChildProcessError(f"Command failed status:{o}")
            self.log.info("END OF EXECUTION")
            pass


class Ec2Terminator(BaseEc2Operator):
    ui_color = "#bdb2ff"

    @apply_defaults
    def __init__(self,  terminate='stop',  *args, **kwargs):
        """
        This operator search all instances matching the (tag_key, tag value) combination, and terminates or stop them.
        :param terminate: if 'stop', stop instance Else if 'terminate', terminate instance'
        :param args: See args from Ec2BaseOperator and BaseOperator
        :param kwargs:
        """
        super(Ec2Terminator, self).__init__( *args, **kwargs)
        self.terminate = terminate

    def execute(self, context):
        """
        This operator search all instances matching the (tag_key, tag value) combination, and terminates or stop them.
        :param context:
        :return:
        """
        self.log.info(f"Executing Operator Ec2Terminator")
        self.log.info(f"TAG_KEY {self.tag_key} TAG_VALUE {self.tag_value}")
        ecr = self._get_ecr()
        InstanceIds = self._filter_per_tag_per_status(state='available')
        if len(InstanceIds) > 0:
            self.log.info(f"Available Instances found : {'; '.join(InstanceIds)}")
            n = 0
            while n <= self.retry:
                n += 1
                assert isinstance(InstanceIds, list)
                instances_status = [(c_id, self._get_instance_status(c_id)) for c_id in InstanceIds]
                active_instances = [c_id for c_id, c_stat in instances_status if c_stat in ['available', 'modifying']]
                self.log.info(f"Try {n} of {self.retry}: Available Instances found : {'; '.join(InstanceIds)}")
                if len(active_instances) > 0:
                    if self.terminate == 'terminate':
                        m = ecr.instances.filter(InstanceIds=active_instances).terminate()
                    elif self.terminate == 'stop':
                        m = ecr.instances.filter(InstanceIds=active_instances).stop()
                    else:
                        raise ValueError(f"terminate parameter must be either 'terminate' or 'stop'")
                    time.sleep(self.sleep)
                else:
                    break
        all_instances_final = self._filter_per_tag()
        all_instances_final_status = [(c_id, self._get_instance_status(c_id)) for c_id in all_instances_final]
        all_instances_status = [", ".join(c) for c in all_instances_final_status]
        s = ";\n"
        self.log.info(f"Final Instances found:\n{s.join(all_instances_status)}")
        self.log.info("END OF EXECUTION")

