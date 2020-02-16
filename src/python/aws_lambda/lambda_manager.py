import boto3
import os

from static.static_variables import StaticVariables


class LambdaManager(object):
    def __init__(self, lambda_client, s3, region, code_path, job_id, filename, handler,
                 memory_limit=StaticVariables.LAMBDA_MEMORY_LIMIT):
        self.lambda_client = lambda_client
        self.region = StaticVariables.DEFAULT_REGION if region is None else region
        self.s3 = s3
        self.code_file = code_path
        self.job_id = job_id
        self.function_name = filename
        self.handler = handler
        self.role = os.environ.get("serverless_mapreduce_role")
        # TODO: remove this if above works
        # self.role = "arn:aws:iam::990092034516:role/serverless_mr_role"
        self.memory = memory_limit
        self.timeout = 300
        self.function_arn = None  # set after creation

    # TracingConfig parameter switches X-Ray tracing on/off.
    # Change value to 'Mode':'PassThrough' to switch it off
    def create_lambda_function(self):
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
                    "serverless_mapreduce_role": self.role
                }
            },
            MemorySize=self.memory,
            Timeout=self.timeout,
            TracingConfig={'Mode': 'PassThrough'}
        )
        self.function_arn = response['FunctionArn']
        print(response)

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
        print(response)

    def update_code_or_create_on_no_exist(self):
        """
        Update if the function exists, else create function
        """
        try:
            self.create_lambda_function()
        except Exception as e:
            # parse (Function already exist)
            print("Failed to create or update lambda:", e)
            self.update_function()

    def add_lambda_permission(self, s_id, bucket):
        response = self.lambda_client.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName=self.function_name,
            Principal='s3.amazonaws.com',
            StatementId='%s' % s_id,
            SourceArn='arn:aws:s3:::' + bucket
        )
        print(response)

    def create_s3_event_source_notification(self, bucket, prefix):
        self.s3.put_bucket_notification_configuration(
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

    def delete_function(self):
        self.lambda_client.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        """
        Delete all Lambda log group and log streams for a given function
        """
        log_client = boto3.client('logs')
        # response = log_client.describe_log_streams(logGroupName='/aws/aws_lambda/' + func_name)
        response = log_client.delete_log_group(logGroupName='/aws/aws_lambda/' + func_name)
        return response
