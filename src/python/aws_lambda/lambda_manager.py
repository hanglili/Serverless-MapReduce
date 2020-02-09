import boto3
import botocore
import os


class LambdaManager(object):
    def __init__(self, l, s3, region, codepath, job_id, fname, handler, lmem=1536):
        self.aws_lambda = l
        self.region = "us-east-1" if region is None else region
        self.s3 = s3
        self.code_file = codepath
        self.job_id = job_id
        self.function_name = fname
        self.handler = handler
        self.role = os.environ.get('serverless_mapreduce_role')
        self.memory = lmem
        self.timeout = 300
        self.function_arn = None  # set after creation

    # TracingConfig parameter switches X-Ray tracing on/off.
    # Change value to 'Mode':'PassThrough' to switch it off
    def create_lambda_function(self):
        runtime = 'python3.7'
        response = self.aws_lambda.create_function(
            FunctionName=self.function_name,
            Code={
                "ZipFile": open(self.code_file, 'rb').read()
            },
            Handler=self.handler,
            Role=self.role,
            Runtime=runtime,
            Description=self.function_name,
            MemorySize=self.memory,
            Timeout=self.timeout,
            TracingConfig={'Mode': 'PassThrough'}
        )
        self.function_arn = response['FunctionArn']
        print(response)

    def update_function(self):
        '''
        Update aws_lambda function
        '''
        response = self.aws_lambda.update_function_code(
            FunctionName=self.function_name,
            ZipFile=open(self.code_file, 'rb').read(),
            Publish=True
        )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n)
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn
        print(response)

    def update_code_or_create_on_noexist(self):
        '''
        Update if the function exists, else create function
        '''
        try:
            self.create_lambda_function()
        except botocore.exceptions.ClientError as e:
            # parse (Function already exist)
            self.update_function()

    def add_lambda_permission(self, sId, bucket):
        response = self.aws_lambda.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName=self.function_name,
            Principal='s3.amazonaws.com',
            StatementId='%s' % sId,
            SourceArn='arn:aws:s3:::' + bucket
        )
        print(response)

    def create_s3_eventsource_notification(self, bucket, prefix=None):
        if not prefix:
            prefix = self.job_id + "/task"

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
        self.aws_lambda.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        '''
        Delete all Lambda log group and log streams for a given function

        '''
        log_client = boto3.client('logs')
        # response = log_client.describe_log_streams(logGroupName='/aws/aws_lambda/' + func_name)
        response = log_client.delete_log_group(logGroupName='/aws/aws_lambda/' + func_name)
        return response
