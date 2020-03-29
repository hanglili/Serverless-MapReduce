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
        if self.static_job_info['localTesting']:
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                          region_name=StaticVariables.DEFAULT_REGION,
                                          endpoint_url='http://localhost:4572')
        else:
            self.s3_client = boto3.client('s3')

    # Serverless set up
    def register_driver(self):
        # init
        # bucket = self.config["bucket"]
        region = self.config["region"]
        # lambda_memory = self.config["lambdaMemory"]
        # concurrent_lambdas = self.config["concurrentLambdas"]
        lambda_read_timeout = self.config["lambdaReadTimeout"]
        boto_max_connections = self.config["botoMaxConnections"]
        # job_bucket = self.config["jobBucket"]
        # num_reducers = self.config["numReducers"]

        driver_lambda_name = self.static_job_info["driverFunction"]
        job_id = self.static_job_info["jobId"]

        zip.zip_lambda(self.config["mapper"]["name"], self.config["mapper"]["zip"])
        zip.zip_lambda(self.config["reducer"]["name"], self.config["reducer"]["zip"])
        zip.zip_lambda(self.config["reducerCoordinator"]["name"], self.config["reducerCoordinator"]["zip"])

        zip.zip_driver_lambda(self.config["driver"]["zip"])

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        lambda_config = Config(read_timeout=lambda_read_timeout,
                               max_pool_connections=boto_max_connections,
                               region_name=region)
        lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                     region_name=StaticVariables.DEFAULT_REGION,
                                     endpoint_url='http://localhost:4574', config=lambda_config)
        l_driver = lambda_manager.LambdaManager(lambda_client, self.s3_client, region,
                                                self.config["driver"]["zip"], job_id,
                                                driver_lambda_name, self.config["driver"]["handler"])
        l_driver.update_code_or_create_on_no_exist()

    def invoke(self):
        # init
        region = self.config["region"]
        lambda_read_timeout = self.config["lambdaReadTimeout"]
        boto_max_connections = self.config["botoMaxConnections"]

        driver_lambda_name = self.static_job_info["driverFunction"]

        lambda_config = Config(read_timeout=lambda_read_timeout,
                               max_pool_connections=boto_max_connections,
                               region_name=region)
        lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                     region_name=StaticVariables.DEFAULT_REGION,
                                     endpoint_url='http://localhost:4574', config=lambda_config)

        result = lambda_client.invoke(
            FunctionName=driver_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        print("Finished executing this job: ", result)