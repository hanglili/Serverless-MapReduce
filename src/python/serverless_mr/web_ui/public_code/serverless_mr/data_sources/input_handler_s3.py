import boto3
import os
import logging

from static.static_variables import StaticVariables

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.input-handler-s3')


class InputHandlerS3:

    def __init__(self, in_lambda, is_local_testing):
        if is_local_testing:
            if in_lambda:
                local_endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4572'
            self.client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('s3')

    def set_up_local_input_data(self, input_file_paths, static_job_info):
        input_bucket = static_job_info[StaticVariables.INPUT_SOURCE_FN]

        self.client.create_bucket(Bucket=input_bucket)
        s3_bucket_exists_waiter = self.client.get_waiter('bucket_exists')
        s3_bucket_exists_waiter.wait(Bucket=input_bucket)
        self.client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=input_bucket,
        )

        for i in range(len(input_file_paths)):
            input_file_path = input_file_paths[i]
            if os.path.isdir(input_file_path):
                continue
            if StaticVariables.INPUT_PREFIX_FN in static_job_info:
                prefix = static_job_info[StaticVariables.INPUT_PREFIX_FN]
                key = '%s/input-%s' % (prefix, str(i + 1))
            else:
                key = 'input-%s' % (str(i + 1))
            self.client.upload_file(Filename=input_file_path,
                                    Bucket=input_bucket,
                                    Key=key)

        logger.info("Set up local input data successfully")

    def get_all_input_keys(self, static_job_info):
        # Returns all input keys to be processed: a list of format obj where obj is a map of {'Key': ..., 'Size': ...}
        all_keys = []
        input_source = static_job_info[StaticVariables.INPUT_SOURCE_FN]
        if StaticVariables.INPUT_PREFIX_FN in static_job_info:
            contents = self.client.list_objects(Bucket=input_source,
                                                Prefix=static_job_info[StaticVariables.INPUT_PREFIX_FN])['Contents']
        else:
            contents = self.client.list_objects(Bucket=input_source)['Contents']
        for obj in contents:
            if not obj['Key'].endswith('/'):
                all_keys.append(obj)

        return all_keys

    def read_value(self, input_source, input_key, static_job_info):
        response = self.client.get_object(Bucket=input_source, Key=input_key)
        contents = response['Body'].read()

        return contents.decode("utf-8")
