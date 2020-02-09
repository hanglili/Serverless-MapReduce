import boto3
import json
import resource
import time

# create an S3 & Dynamo session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
TASK_MAPPER_PREFIX = "task/mapper/"
TASK_REDUCER_PREFIX = "task/reducer/"


def write_to_s3(bucket, key, data, metadata):
    # Write to S3 Bucket
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def reduce_handler(reduce_function):
    def lambda_handler(event, context):

        start_time = time.time()

        job_bucket = event['jobBucket']
        bucket = event['bucket']
        reducer_keys = event['keys']
        job_id = event['jobId']
        r_id = event['reducerId']
        step_id = event['stepId']
        n_reducers = event['nReducers']

        # aggr
        results = {}
        line_count = 0

        # INPUT JSON => OUTPUT JSON

        # Download and process all keys
        for key in reducer_keys:
            response = s3_client.get_object(Bucket=job_bucket, Key=key)
            contents = response['Body'].read()

            line_count += reduce_function(results, contents)

        time_in_secs = (time.time() - start_time)
        # timeTaken = time_in_secs * 1000000000 # in 10^9
        # s3DownloadTime = 0
        # totalProcessingTime = 0
        pret = [len(reducer_keys), line_count, time_in_secs]
        print("Reducer output", pret)

        if n_reducers == 1:
            # Last reducer file, final result
            fname = "%s/result" % job_id
        else:
            fname = "%s/%s%s/%s" % (job_id, TASK_REDUCER_PREFIX, step_id, r_id)

        metadata = {
            "linecount": '%s' % line_count,
            "processingtime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }

        write_to_s3(job_bucket, fname, json.dumps(results), metadata)
        return pret

    return lambda_handler
