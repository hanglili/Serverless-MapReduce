import boto3
import json
import resource
import time
import os
import pickle

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import output_handler


def lambda_handler(event, _):
    print("**************Reduce****************")
    start_time = time.time()

    reduce_keys = event['keys']
    reducer_id = event['id']
    reduce_function_pickle_path = event['function_pickle_path']

    with open(reduce_function_pickle_path, 'rb') as f:
        reduce_function = pickle.load(f)

    # create an S3 & Dynamo session
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
        s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                 region_name=StaticVariables.DEFAULT_REGION,
                                 endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
    else:
        s3_client = boto3.client('s3')

    shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
    use_combine = static_job_info[StaticVariables.USE_COMBINE_FLAG_FN]
    job_name = static_job_info[StaticVariables.JOB_NAME_FN]

    stage_id = int(os.environ.get("stage_id"))
    total_num_stages = int(os.environ.get("total_num_stages"))

    print("Stage:", stage_id)

    # aggr
    line_count = 0
    intermediate_data = []

    # INPUT JSON => OUTPUT JSON

    # Download and process all keys
    for key in reduce_keys:
        response = s3_client.get_object(Bucket=shuffling_bucket, Key=key)
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

    metadata = {
        "lineCount": '%s' % line_count,
        "processingTime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "numKeys": '%s' % len(reduce_keys)
    }

    if stage_id == total_num_stages:
        cur_output_handler = output_handler.get_output_handler(static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                               in_lambda=True)
        cur_output_handler.write_output(reducer_id, outputs, metadata, static_job_info)
    else:
        mapper_filename = "%s/%s-%s/%s" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, reducer_id)
        s3_client.put_object(Bucket=shuffling_bucket, Key=mapper_filename,
                             Body=json.dumps(outputs), Metadata=metadata)
