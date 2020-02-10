import boto3
import json
import resource
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

from job.partition import partition

# constants
TASK_MAPPER_PREFIX = "task/mapper/"
JOB_INFO = "configuration/job-info.json"


def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def map_handler(map_function):
    def lambda_handler(event, context):
        start_time = time.time()

        job_bucket = event['jobBucket']
        src_bucket = event['bucket']
        src_keys = event['keys']
        job_id = event['jobId']
        mapper_id = event['mapperId']

        config = json.loads(open(JOB_INFO, "r").read())

        num_bins = config["reduceCount"]

        # aggr
        outputs = {}
        line_count = 0
        err = ''

        # INPUT CSV => OUTPUT JSON

        outputs = []

        # Download and process all keys
        for key in src_keys:
            response = s3_client.get_object(Bucket=src_bucket, Key=key)
            contents = response['Body'].read()

            for line in contents.decode("utf-8").split('\n')[:-1]:
                line_count += 1
                cur_input_pair = (key, line)
                cur_line_outputs = []
                map_function(cur_line_outputs, cur_input_pair)
                outputs += cur_line_outputs

        time_in_secs = (time.time() - start_time)
        # timeTaken = time_in_secs * 1000000000 # in 10^9
        # s3DownloadTime = 0
        # totalProcessingTime = 0
        pret = [len(src_keys), line_count, time_in_secs, err]

        metadata = {
            "linecount": '%s' % line_count,
            "processingtime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }
        print("metadata: ", metadata)

        # Partition ids are from 1 to n (inclusive).
        output_partitions = [dict() for _ in range(num_bins + 1)]

        for key, value in output:
            # partition_id = "partition%s" % partition(key)
            partition_id = partition(key)
            output_partitions[partition_id][key] = value

        for i in range(1, num_bins + 1):
            partition_id = "bin%s" % i
            mapper_fname = "%s/%s%s/%s" % (job_id, TASK_MAPPER_PREFIX, partition_id, mapper_id)
            write_to_s3(job_bucket, mapper_fname, json.dumps(output_partitions[i]), metadata)

        return pret

    return lambda_handler
