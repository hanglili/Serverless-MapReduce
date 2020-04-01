import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables


class InputHandlerS3:

    def __init__(self, in_lambda):
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if in_lambda:
                local_endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4572'
            self.client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('s3')

    def set_up_local_input_data(self, input_file_paths):
        input_bucket = self.static_job_info[StaticVariables.INPUT_SOURCE_FN]
        prefix = self.static_job_info[StaticVariables.INPUT_PREFIX_FN]

        self.client.create_bucket(Bucket=input_bucket)
        self.client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=input_bucket,
        )

        for i in range(len(input_file_paths)):
            input_file_path = input_file_paths[i]
            key = '%s-input-%s' % (prefix, str(i + 1))
            self.client.upload_file(Filename=input_file_path,
                                    Bucket=input_bucket,
                                    Key=key)

        print("Set up local input data successfully")

    def get_all_input_keys(self):
        # Returns all input keys to be processed: a list of format obj where obj is a map of {'Key': ..., 'Size': ...}
        all_keys = []
        input_source = self.static_job_info[StaticVariables.INPUT_SOURCE_FN]
        for obj in self.client.list_objects(Bucket=input_source,
                                            Prefix=self.static_job_info[StaticVariables.INPUT_PREFIX_FN])['Contents']:
            if not obj['Key'].endswith('/'):
                all_keys.append(obj)

        return all_keys

    def read_records_from_input_key(self, input_key):
        input_source = self.static_job_info[StaticVariables.INPUT_SOURCE_FN]
        response = self.client.get_object(Bucket=input_source, Key=input_key)
        contents = response['Body'].read()

        return contents.decode("utf-8").split('\n')[:-1]
