import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.main import ServerlessMR
from user_job_5.map import extract_data_s3
from user_job_5.map_2 import truncate_decimals
from user_job_5.reduce import reduce_function
from user_job_5.partition import partition
from unittest import TestCase


class Test(TestCase):

    static_job_info_path = "configuration/static-job-info.json"

    @classmethod
    def setUp(self):
        print('\r\nSetting up and executing the job')
        self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                      region_name=StaticVariables.DEFAULT_REGION,
                                      endpoint_url='http://localhost:4572')
        static_job_info_file = open(Test.static_job_info_path, 'r')
        self.static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()
        os.environ["serverless_mapreduce_role"] = 'dummy-role'

    @classmethod
    def tearDown(self):
        print('\r\nTearing down the job')
        del os.environ["serverless_mapreduce_role"]

    def test_that_lambda_returns_correct_message(self):
        # Execute the job
        serverless_mr = ServerlessMR()
        submission_time = serverless_mr.map(extract_data_s3).map(truncate_decimals)\
            .shuffle(partition).reduce(reduce_function, 4).run()

        print("The job has finished")

        # Output
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        output_bucket = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        output_prefix = self.static_job_info[StaticVariables.OUTPUT_PREFIX_FN]
        output_bin = 3
        output_full_prefix = "%s/%s/%s/%s" % (job_name, output_prefix, submission_time, str(output_bin))
        response = self.s3_client.get_object(Bucket=output_bucket, Key=output_full_prefix)
        contents = response['Body'].read()
        results = json.loads(contents)

        # Assertions
        self.assertEqual(results[0][0], '0.0.0.0')
        self.assertEqual(results[0][1], 80.0)
