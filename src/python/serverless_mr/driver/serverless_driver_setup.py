import json
import boto3
import pickle
import os
import glob

from datetime import datetime
from driver.driver import set_up_local_input_data, pickle_functions_and_zip_stage
from static.static_variables import StaticVariables
from utils import zip
from aws_lambda import lambda_manager
from botocore.client import Config


def delete_files(filenames):
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)


def overwrite_existing_job_info(pipeline_specific_config):
    with open(StaticVariables.STATIC_JOB_INFO_PATH, "r") as f:
        cur_config = json.load(f)

    for key, value in pipeline_specific_config.items():
        cur_config[key] = value

    with open(StaticVariables.STATIC_JOB_INFO_PATH, "w") as f:
        json.dump(cur_config, f)

    return cur_config


class ServerlessDriverSetup:
    def __init__(self, pipelines, total_num_functions):
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
        self.driver_lambda_name = "%s-%s-%s" % (self.job_name, lambda_name_prefix, "driver")

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

        self.pipelines = pipelines
        self.total_num_functions = total_num_functions
        self.set_up_bucket(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME)

    def set_up_bucket(self, bucket_name):
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.s3_client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=bucket_name,
        )
        print("%s Bucket created successfully" % bucket_name)

    # Serverless set up
    def register_driver(self):
        stage_id = 1
        function_filepaths = []

        # The first function should be a map/map_shuffle function
        for pipeline_id, pipeline in self.pipelines.items():
            functions = pipeline.get_functions()
            pipeline_static_job_info = overwrite_existing_job_info(pipeline.get_config())
            self.static_job_info = pipeline_static_job_info
            dependent_pipeline_ids = pipeline.get_dependent_pipeline_ids()
            if len(dependent_pipeline_ids) == 0:
                set_up_local_input_data(pipeline_static_job_info)
            for i in range(len(functions)):
                cur_function = functions[i]
                cur_function_zip_path = "%s-%s.zip" % (cur_function.get_string(), stage_id)

                # Prepare Lambda functions if driver running in local machine
                rel_function_paths = pickle_functions_and_zip_stage(cur_function_zip_path, cur_function, stage_id)

                function_filepaths += rel_function_paths
                stage_id += 1

        with open(StaticVariables.SERVERLESS_PIPELINES_INFO_PATH, 'wb') as f:
            pickle.dump(self.pipelines, f)

        zip.zip_lambda([StaticVariables.COORDINATOR_HANDLER_PATH], StaticVariables.COORDINATOR_ZIP_PATH)

        zip.zip_driver_lambda(StaticVariables.DRIVER_ZIP_PATH, function_filepaths)

        serverless_driver = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, self.region,
                                                         StaticVariables.DRIVER_ZIP_PATH,
                                                         self.job_name, self.driver_lambda_name,
                                                         StaticVariables.SERVERLESS_DRIVER_HANDLER_FUNCTION_PATH)
        serverless_driver.update_code_or_create_on_no_exist(self.total_num_functions)

        registered_job_information = {'jobName': self.job_name, 'driverLambdaName': self.driver_lambda_name,
                                      'registeredTime': str(datetime.utcnow()),
                                      'shufflingBucket': self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN],
                                      'inputSource': self.static_job_info[StaticVariables.INPUT_SOURCE_FN],
                                      "outputSource": self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN],
                                      'totalNumPipelines': len(self.pipelines), 'totalNumStages': stage_id - 1}
        self.s3_client.put_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                  Key=(StaticVariables.S3_UI_REGISTERED_JOB_INFORMATION_PATH % self.job_name),
                                  Body=json.dumps(registered_job_information))
        self.s3_client.put_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                  Key=(StaticVariables.S3_UI_REGISTERED_JOB_DRIVER_CONFIG_PATH % self.job_name),
                                  Body=json.dumps(self.config))

        delete_files(glob.glob(StaticVariables.FUNCTIONS_PICKLE_GLOB_PATH))
        delete_files(glob.glob(StaticVariables.LAMBDA_ZIP_GLOB_PATH))

    def invoke(self):
        result = self.lambda_client.invoke(
            FunctionName=self.driver_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )

        print("Finished executing this job: ", result)
