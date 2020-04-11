import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables


class OutputHandlerS3:

    def __init__(self, in_lambda):
        static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if in_lambda:
                local_endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4572'
            self.client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('s3')

    def write_output(self, reducer_id, outputs, metadata, static_job_info):
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        reduce_output_full_prefix = "%s/%s" % (job_name, StaticVariables.REDUCE_OUTPUT_PREFIX_S3) \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]

        output_file_name = "%s/%s" % (reduce_output_full_prefix, reducer_id)
        self.client.put_object(Bucket=output_source, Key=output_file_name, Body=json.dumps(outputs), Metadata=metadata)

    def list_objects_for_checking_finish(self, static_job_info):
        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info else static_job_info[
            StaticVariables.OUTPUT_SOURCE_FN]
        reduce_output_full_prefix = "%s/%s" % (job_name, StaticVariables.REDUCE_OUTPUT_PREFIX_S3) \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]

        return self.client.list_objects(Bucket=output_source, Prefix=reduce_output_full_prefix), "Contents"

    def check_job_finish(self, response, string_index, num_final_dst_operators, static_job_info):
        shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        output_bucket = static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        reducer_lambda_time = 0
        reducer_ids = response[string_index]
        if len(reducer_ids) == num_final_dst_operators:
            job_keys = self.client.list_objects(Bucket=shuffling_bucket, Prefix=job_name)["Contents"]
            total_s3_size = sum([job_key["Size"] for job_key in job_keys])
            for reducer_id in reducer_ids:
                # Even though metadata processing time is written as processingTime,
                # AWS does not accept uppercase letter metadata key
                reducer_lambda_time += float(self.client.get_object(Bucket=output_bucket, Key=reducer_id["Key"])
                                             ['Metadata']['processingtime'])
                total_s3_size += reducer_id["Size"]

            return reducer_lambda_time, total_s3_size, len(job_keys)
        return -1, -1, -1

    def get_output(self, reducer_id, static_job_info):
        output_source = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN] \
            if StaticVariables.OUTPUT_SOURCE_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        job_name = static_job_info[StaticVariables.JOB_NAME_FN]
        reduce_output_full_prefix = "%s/%s" % (job_name, StaticVariables.REDUCE_OUTPUT_PREFIX_S3) \
            if StaticVariables.OUTPUT_PREFIX_FN not in static_job_info \
            else static_job_info[StaticVariables.OUTPUT_PREFIX_FN]

        output_file_name = "%s/%s" % (reduce_output_full_prefix, reducer_id)

        response = self.client.get_object(Bucket=output_source, Key=output_file_name)
        contents = response['Body'].read()

        return contents.decode("utf-8")
