import json
import boto3

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import zip
from serverless_mr.aws_lambda import lambda_manager
from botocore.client import Config


class ServerlessDriverSetup:
    def __init__(self):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                          region_name=StaticVariables.DEFAULT_REGION,
                                          endpoint_url='http://localhost:4572')
        else:
            self.s3_client = boto3.client('s3')

        self.region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        lambda_read_timeout = self.config[StaticVariables.LAMBDA_READ_TIMEOUT_FN] \
            if StaticVariables.LAMBDA_READ_TIMEOUT_FN in self.config else StaticVariables.DEFAULT_LAMBDA_READ_TIMEOUT
        boto_max_connections = self.config[StaticVariables.BOTO_MAX_CONNECTIONS_FN] \
            if StaticVariables.BOTO_MAX_CONNECTIONS_FN in self.config else StaticVariables.DEFAULT_BOTO_MAX_CONNECTIONS
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        self.job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        self.driver_lambda_name = lambda_name_prefix + "-driver-" + self.job_name

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        lambda_config = Config(read_timeout=lambda_read_timeout,
                               max_pool_connections=boto_max_connections,
                               region_name=self.region)
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            self.lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                         region_name=self.region,
                                         endpoint_url='http://localhost:4574', config=lambda_config)
        else:
            self.lambda_client = boto3.client('lambda', config=lambda_config)

    # Serverless set up
    def register_driver(self):
        zip.zip_lambda(self.config[StaticVariables.MAPPER_FN][StaticVariables.LOCATION_FN],
                       self.config[StaticVariables.MAPPER_FN][StaticVariables.ZIP_FN])
        zip.zip_lambda(self.config[StaticVariables.REDUCER_FN][StaticVariables.LOCATION_FN],
                       self.config[StaticVariables.REDUCER_FN][StaticVariables.ZIP_FN])
        zip.zip_lambda(self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.LOCATION_FN],
                       self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.ZIP_FN])

        zip.zip_driver_lambda(self.config[StaticVariables.DRIVER_FN][StaticVariables.ZIP_FN])

        l_driver = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, self.region,
                                                self.config[StaticVariables.DRIVER_FN][StaticVariables.ZIP_FN],
                                                self.job_name, self.driver_lambda_name,
                                                self.config[StaticVariables.DRIVER_FN][StaticVariables.HANDLER_FN])
        l_driver.update_code_or_create_on_no_exist()

    def invoke(self):
        result = self.lambda_client.invoke(
            FunctionName=self.driver_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        print("Finished executing this job: ", result)