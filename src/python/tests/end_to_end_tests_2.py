import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.main import ServerlessMR

from word_count_map import produce_counts
from word_count_reduce import aggregate_counts
from unittest import TestCase


class Test(TestCase):

    @classmethod
    def setUp(self):
        print('\r\nSetting up and executing the job')
        self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                      region_name=StaticVariables.DEFAULT_REGION,
                                      endpoint_url='http://localhost:4572')
        static_job_info_path = "configuration/static-job-info.json"
        static_job_info_file = open(static_job_info_path, 'r')
        self.static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()
        os.environ["serverless_mapreduce_role"] = 'dummy-role'

    @classmethod
    def tearDown(self):
        print('\r\nTearing down the job')
        del os.environ["serverless_mapreduce_role"]

    def test_that_word_count_returns_correct_results(self):
        # Execute the job
        serverless_mr = ServerlessMR()
        submission_time = serverless_mr.map(produce_counts).reduce(aggregate_counts, 4).run()

        print("Job terminated")

        # Retrieve the outputs produced by reducer 2
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        output_bucket = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        output_bin = 2
        output_full_prefix = "%s/stage/%s/%s" % (job_name, submission_time, str(output_bin))
        response = self.s3_client.get_object(Bucket=output_bucket, Key=output_full_prefix)
        contents = response['Body'].read()
        results = json.loads(contents)

        # Assertions
        self.assertIn(['Microsoft', 2], results)
        self.assertIn(['IBM', 2], results)
        self.assertIn(['Software', 1], results)
