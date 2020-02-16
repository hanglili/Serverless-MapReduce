import sys
import boto3
import json

from aws_lambda import lambda_manager
from driver.driver import Driver
from botocore.client import Config
from utils import zip, access_s3
from static.static_variables import StaticVariables

JOB_ID = "bl-release"
L_PREFIX = "BL"
driver_lambda_name = L_PREFIX + "-driver-" + JOB_ID
reducer_lambda_name = L_PREFIX + "-reducer-" + JOB_ID


def register_and_invoke_driver():
    s3_client = boto3.client('s3')
    config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
    # init
    # bucket = config["bucket"]
    region = config["region"]
    # lambda_memory = config["lambdaMemory"]
    # concurrent_lambdas = config["concurrentLambdas"]
    lambda_read_timeout = config["lambdaReadTimeout"]
    boto_max_connections = config["botoMaxConnections"]
    job_bucket = config["jobBucket"]
    num_reducers = config["numReducers"]
    # TODO: Substitute num_mappers with something else
    num_mappers = 5

    # write job self.config
    access_s3.write_job_config(JOB_ID, job_bucket, num_mappers, reducer_lambda_name,
                               config["reducer"]["handler"], num_reducers)

    zip.zip_lambda(config["mapper"]["name"], config["mapper"]["zip"])
    zip.zip_lambda(config["reducer"]["name"], config["reducer"]["zip"])
    zip.zip_lambda(config["reducerCoordinator"]["name"], config["reducerCoordinator"]["zip"])

    zip.zip_driver_lambda(config["driver"]["zip"])

    # Setting longer timeout for reading aws_lambda results and larger connections pool
    lambda_config = Config(read_timeout=lambda_read_timeout,
                           max_pool_connections=boto_max_connections,
                           region_name=region)
    lambda_client = boto3.client('lambda', config=lambda_config)
    l_driver = lambda_manager.LambdaManager(lambda_client, s3_client, region,
                                            config["driver"]["zip"], JOB_ID,
                                            driver_lambda_name, config["driver"]["handler"])
    l_driver.update_code_or_create_on_no_exist()

    lambda_client.invoke(
        FunctionName=driver_lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({})
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Wrong number of arguments.")
    else:
        mode = sys.argv[1]

        if int(mode) == 0:
            driver = Driver()
            driver.run()
        else:
            register_and_invoke_driver()
