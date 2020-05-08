import boto3
import os
import json
import logging

from static.static_variables import StaticVariables
from utils.setup_logger import logger

logger = logging.getLogger('serverless-mr.lambda-manager')


class LambdaManager(object):
    def __init__(self, lambda_client, s3, region, code_path, job_id, function_name, handler,
                 memory_limit=StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT):
        self.lambda_client = lambda_client
        self.region = region
        self.s3 = s3
        self.code_file = code_path
        self.job_id = job_id
        self.function_name = function_name
        self.handler = handler
        self.role = os.environ.get("serverless_mapreduce_role")
        self.memory = memory_limit
        self.timeout = StaticVariables.DEFAULT_LAMBDA_TIMEOUT
        self.function_arn = None  # set after creation

    # TracingConfig parameter switches X-Ray tracing on/off.
    # Change value to 'Mode':'PassThrough' to switch it off
    def create_lambda_function(self, stage_id, total_num_stages, submission_time, coordinator_lambda_name, num_reducers):
        runtime = 'python3.7'
        response = self.lambda_client.create_function(
            FunctionName=self.function_name,
            Code={
                "ZipFile": open(self.code_file, 'rb').read()
            },
            Handler=self.handler,
            Role=self.role,
            Runtime=runtime,
            Description=self.function_name,
            Environment={
                'Variables': {
                    "serverless_mapreduce_role": self.role,
                    "stage_id": str(stage_id),
                    "total_num_stages": str(total_num_stages),
                    'submission_time': str(submission_time),
                    'coordinator_lambda_name': str(coordinator_lambda_name),
                    "num_reducers": str(num_reducers)
                }
            },
            MemorySize=self.memory,
            Timeout=self.timeout,
            TracingConfig={'Mode': 'PassThrough'}
        )
        self.function_arn = response['FunctionArn']
        logger.info("Creation of Lambda function %s - response: %s" % (self.function_name, response))

    def update_function(self):
        """
        Update aws_lambda function
        """
        response = self.lambda_client.update_function_code(
            FunctionName=self.function_name,
            ZipFile=open(self.code_file, 'rb').read(),
            Publish=True
        )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n)
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn
        logger.info("Update of Lambda function %s - response: %s" % (self.function_name, response))

    def update_code_or_create_on_no_exist(self, total_num_stages, submission_time="",
                                          coordinator_lambda_name="", stage_id=-1, num_reducers=-1):
        """
        Update if the function exists, else create function
        """
        try:
            self.create_lambda_function(stage_id, total_num_stages, submission_time, coordinator_lambda_name, num_reducers)
        except Exception as e:
            # parse (Function already exist)
            logger.warning("Failed to create Lambda function %s: %s" % (self.function_name, e))
            self.update_function()

    def add_lambda_permission(self, s_id, bucket):
        try:
            response = self.lambda_client.add_permission(
                Action='lambda:InvokeFunction',
                FunctionName=self.function_name,
                Principal='s3.amazonaws.com',
                StatementId='%s' % s_id,
                SourceArn='arn:aws:s3:::' + bucket
            )
            logger.info("Adding permission to Lambda function %s - response: %s" % (self.function_name, response))
        except Exception as e:
            logger.info("Failed to add permission to Lambda function %s: %s" % (self.function_name, e))

    def create_s3_event_source_notification(self, bucket, prefix):
        response = self.s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'Events': ['s3:ObjectCreated:*'],
                        'LambdaFunctionArn': self.function_arn,
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'prefix',
                                        'Value': prefix
                                    },
                                ]
                            }
                        }
                    }
                ],
                # 'TopicConfigurations' : [],
                # 'QueueConfigurations' : []
            }
        )
        logger.info("Creating S3 event notification to Lambda function %s - response: %s" %
                    (self.function_name, response))

    def delete_function(self):
        self.lambda_client.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        """
        Delete all Lambda log group and log streams for a given function
        """
        static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            log_client = boto3.client('logs', aws_access_key_id='', aws_secret_access_key='',
                                      region_name=StaticVariables.DEFAULT_REGION,
                                      endpoint_url='http://localhost:4586')
        else:
            log_client = boto3.client('logs')

        # response = log_client.describe_log_streams(logGroupName='/aws/aws_lambda/' + func_name)
        response = log_client.delete_log_group(logGroupName='/aws/aws_lambda/' + func_name)
        return response
