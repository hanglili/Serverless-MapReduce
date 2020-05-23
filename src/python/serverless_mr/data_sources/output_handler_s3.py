import boto3
import json
import os
import time
import logging

from static.static_variables import StaticVariables

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.output-handler-s3')


class OutputHandlerS3:

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

    def create_output_storage(self, static_job_info, submission_time):
        output_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        self.client.create_bucket(Bucket=output_bucket)
        s3_bucket_exists_waiter = self.client.get_waiter('bucket_exists')
        s3_bucket_exists_waiter.wait(Bucket=output_bucket)
        if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            self.client.put_bucket_acl(
                ACL='public-read-write',
                Bucket=output_bucket,
            )
        logger.info("Finished setting up output bucket")

    def write_output(self, executor_id, outputs, metadata, static_job_info, submission_time):
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        output_prefix = StaticVariables.REDUCE_OUTPUT_PREFIX_S3 \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]
        output_full_prefix = "%s/%s/%s" % (job_name, output_prefix, submission_time)

        output_file_name = "%s/%s" % (output_full_prefix, executor_id)
        self.client.put_object(Bucket=output_source, Key=output_file_name, Body=json.dumps(outputs), Metadata=metadata)

    def list_objects_for_checking_finish(self, static_job_info, submission_time):
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        output_prefix = StaticVariables.REDUCE_OUTPUT_PREFIX_S3 \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]
        output_full_prefix = "%s/%s/%s" % (job_name, output_prefix, submission_time)

        response = self.client.list_objects(Bucket=output_source, Prefix=output_full_prefix)
        if "Contents" in response:
            return response["Contents"]
        else:
            return None

    def check_job_finish(self, last_stage_keys, num_final_dst_operators, static_job_info, submission_time):
        output_bucket = static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        s3_size = 0
        if len(last_stage_keys) == num_final_dst_operators:
            StaticVariables.COST_CALCULATION_START_TIME = time.time()
            job_execution_time = StaticVariables.COST_CALCULATION_START_TIME - StaticVariables.JOB_START_TIME
            logger.info("PERFORMANCE INFO - Job execution time: %s seconds" % str(job_execution_time))
            for last_stage_key in last_stage_keys:
                # Even though metadata processing time is written as processingTime,
                # AWS does not accept uppercase letter metadata key
                # metadata = self.client.head_object(Bucket=output_bucket, Key=last_stage_key["Key"])['Metadata']
                s3_size += last_stage_key["Size"]  # Size is expressed in (int) Bytes

            s3_put_ops = len(last_stage_keys)
            s3_get_ops = 0
            s3_storage_cost = 1 * 0.0000521574022522109 * (s3_size / 1024.0 / 1024.0 / 1024.0)
            # S3 PUT $0.005/1000
            s3_put_cost = s3_put_ops * 0.005 / 1000
            # S3 GET $0.004/10000
            s3_get_cost = s3_get_ops * 0.004 / 10000
            logger.info("Last stage number of write ops: %s" % s3_put_ops)
            logger.info("Last stage number of read ops: %s" % s3_get_ops)

            return 0, s3_storage_cost, s3_put_cost, s3_get_cost
        return -1, -1, -1, -1

    def get_output(self, executor_id, static_job_info, submission_time):
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        output_prefix = StaticVariables.REDUCE_OUTPUT_PREFIX_S3 \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]
        output_full_prefix = "%s/%s/%s" % (job_name, output_prefix, submission_time)

        output_file_name = "%s/%s" % (output_full_prefix, executor_id)

        response = self.client.get_object(Bucket=output_source, Key=output_file_name)
        contents = response['Body'].read()

        return contents.decode("utf-8")
