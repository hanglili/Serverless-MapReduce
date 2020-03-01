import boto3
import json

from static.static_variables import StaticVariables
from main import init_job
from unittest import TestCase


class Test(TestCase):

    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://localhost:4572')
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())

    @classmethod
    def setup_class(cls):
        print('\r\nSetting up and executing the job')

    @classmethod
    def teardown_class(cls):
        print('\r\nTearing down the job')

    def test_that_lambda_returns_correct_message(self):
        # Execute the job
        init_job(['python3', '0'])
        print("The job has finished")

        # Output
        output_bin = 3
        output_prefix = Test.static_job_info['outputPrefix']
        output_bucket = Test.static_job_info['outputBucket']
        output_filepath = "%s%s" % (output_prefix, str(output_bin))
        response = Test.s3_client.get_object(Bucket=output_bucket, Key=output_filepath)
        contents = response['Body'].read()
        results = json.loads(contents)

        # Assertions
        self.assertEqual(results[0][0], '0.0.0.0')
        self.assertEqual(results[0][1], [50.0])
