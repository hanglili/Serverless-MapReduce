import boto3
import json
import resource
import time
import os

from serverless_mr.static.static_variables import StaticVariables

# create an S3 & Dynamo session
static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info['localTesting']:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
else:
    s3_client = boto3.client('s3')


def write_to_s3(bucket, key, data, metadata):
    # Write to S3 Bucket
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)


def reduce_handler(reduce_function):
    def lambda_handler(event, _):

        start_time = time.time()

        job_bucket = event['jobBucket']
        # bucket = event['bucket']
        reduce_keys = event['keys']
        job_id = event['jobId']
        reducer_id = event['reducerId']
        # step_id = event['stepId']
        # n_reducers = event['numReducers']
        use_combine = event['useCombine']
        output_bucket = event['outputBucket']
        output_prefix = \
            "%s/%s" % (job_id, StaticVariables.REDUCE_OUTPUT_PREFIX) if event['outputPrefix'] == "" \
            else event['outputPrefix']

        # aggr
        line_count = 0
        intermediate_data = []

        # INPUT JSON => OUTPUT JSON

        # Download and process all keys
        for key in reduce_keys:
            response = s3_client.get_object(Bucket=job_bucket, Key=key)
            contents = response['Body'].read()

            for key_value in json.loads(contents):
                line_count += 1
                intermediate_data.append(key_value)

        intermediate_data.sort(key=lambda x: x[0])

        cur_key = None
        cur_values = []
        outputs = []
        for key, value in intermediate_data:
            if cur_key == key:
                if use_combine:
                    cur_values += value
                else:
                    cur_values.append(value)
            else:
                if cur_key is not None:
                    cur_key_outputs = []
                    reduce_function(cur_key_outputs, (cur_key, cur_values))
                    outputs += cur_key_outputs

                cur_key = key
                if use_combine:
                    cur_values = value
                else:
                    cur_values = [value]

        if cur_key is not None:
            cur_key_outputs = []
            reduce_function(cur_key_outputs, (cur_key, cur_values))
            outputs += cur_key_outputs

        time_in_secs = (time.time() - start_time)
        # timeTaken = time_in_secs * 1000000000 # in 10^9
        # s3DownloadTime = 0
        # totalProcessingTime = 0
        processing_info = [len(reduce_keys), line_count, time_in_secs]
        print("Reducer process information: (number of keys processed, line count, processing time)\n", processing_info)

        filename = "%s%s" % (output_prefix, reducer_id)

        metadata = {
            "lineCount": '%s' % line_count,
            "processingTime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }

        write_to_s3(output_bucket, filename, json.dumps(outputs), metadata)
        return processing_info

    lambda_handler.__wrapped__ = reduce_function
    return lambda_handler
