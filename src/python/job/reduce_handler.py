import boto3
import json
import resource
import time

from static.static_variables import StaticVariables

# create an S3 & Dynamo session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def write_to_s3(bucket, key, data, metadata):
    # Write to S3 Bucket
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


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
                cur_values.append(value)
            else:
                if cur_key is not None:
                    cur_key_outputs = []
                    reduce_function(cur_key_outputs, (cur_key, cur_values))
                    outputs += cur_key_outputs

                cur_key = key
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

        filename = "%s/%s%s" % (job_id, StaticVariables.REDUCE_OUTPUT_PREFIX, reducer_id)

        metadata = {
            "lineCount": '%s' % line_count,
            "processingTime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }

        write_to_s3(job_bucket, filename, json.dumps(outputs), metadata)
        return processing_info

    return lambda_handler
