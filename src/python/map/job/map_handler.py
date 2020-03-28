import boto3
import json
import resource
import time
import os

from job.partition import partition
from job.combine import combine_function
from static.static_variables import StaticVariables

# create an S3 session
static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info['localTesting']:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
else:
    s3_client = boto3.client('s3')


def write_to_s3(bucket, key, data, metadata):
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)


def map_handler(map_function):
    def lambda_handler(event, _):
        start_time = time.time()

        job_bucket = event['jobBucket']
        src_bucket = event['bucket']
        src_keys = event['keys']
        job_id = event['jobId']
        mapper_id = event['mapperId']

        num_bins = static_job_info["reduceCount"]
        use_combine = static_job_info["useCombine"]

        # aggr
        line_count = 0
        err = ''

        # INPUT CSV => OUTPUT JSON

        intermediate_data = []

        # Download and process all keys
        for key in src_keys:
            response = s3_client.get_object(Bucket=src_bucket, Key=key)
            contents = response['Body'].read()

            for line in contents.decode("utf-8").split('\n')[:-1]:
                line_count += 1
                cur_input_pair = (key, line)
                cur_line_outputs = []
                map_function(cur_line_outputs, cur_input_pair)
                intermediate_data += cur_line_outputs

        if use_combine:
            intermediate_data.sort(key=lambda x: x[0])

            cur_key = None
            cur_values = []
            outputs = []
            for key, value in intermediate_data:
                if cur_key == key:
                    cur_values.append(value)
                else:
                    if cur_key is not None:
                        cur_key_outputs = []
                        combine_function(cur_key_outputs, (cur_key, cur_values))
                        outputs += cur_key_outputs

                    cur_key = key
                    cur_values = [value]

            if cur_key is not None:
                cur_key_outputs = []
                combine_function(cur_key_outputs, (cur_key, cur_values))
                outputs += cur_key_outputs

        else:
            outputs = intermediate_data

        time_in_secs = (time.time() - start_time)
        # timeTaken = time_in_secs * 1000000000 # in 10^9
        # s3DownloadTime = 0
        # totalProcessingTime = 0
        processing_info = [len(src_keys), line_count, time_in_secs, err]
        print("Mapper process information: (number of keys processed, line count, processing time)\n", processing_info)

        metadata = {
            "lineCount": '%s' % line_count,
            "processingTime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }

        # Partition ids are from 1 to n (inclusive).
        output_partitions = [[] for _ in range(num_bins + 1)]

        for key, value in outputs:
            partition_id = partition(key, num_bins) + 1
            cur_partition = output_partitions[partition_id]
            cur_partition.append(tuple((key, value)))

        for i in range(1, num_bins + 1):
            partition_id = "bin%s" % i
            mapper_filename = "%s/%s%s/%s" % (job_id, StaticVariables.MAP_OUTPUT_PREFIX, partition_id, mapper_id)
            write_to_s3(job_bucket, mapper_filename, json.dumps(output_partitions[i]), metadata)

        return processing_info
    lambda_handler.__wrapped__ = map_function

    return lambda_handler
