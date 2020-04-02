import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.main import init_job, set_up, tear_down
from unittest import TestCase


class Test(TestCase):

    @classmethod
    def setUp(self):
        print('\r\nSetting up and executing the job')
        set_up()
        self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                 region_name=StaticVariables.DEFAULT_REGION,
                                 endpoint_url='http://localhost:4572')
        static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, 'r')
        self.static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()
        os.environ["serverless_mapreduce_role"] = '123'

    @classmethod
    def tearDown(self):
        print('\r\nTearing down the job')
        tear_down()
        del os.environ["serverless_mapreduce_role"]

    def test_that_lambda_returns_correct_message(self):
        # Execute the job
        init_job()
        print("The job has finished")

        # Output
        output_bin = 3
        output_prefix = self.static_job_info[StaticVariables.OUTPUT_PREFIX_FN]
        output_bucket = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        output_filepath = "%s/%s" % (output_prefix, str(output_bin))
        response = self.s3_client.get_object(Bucket=output_bucket, Key=output_filepath)
        contents = response['Body'].read()
        results = json.loads(contents)

        # Assertions
        self.assertEqual(results[0][0], '0.0.0.0')
        self.assertEqual(results[0][1], [80.0])
