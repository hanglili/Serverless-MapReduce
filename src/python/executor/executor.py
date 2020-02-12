import boto3
import json
import subprocess
import resource
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # start_time = time.time()

    task_code = event['taskCode']
    task_bucket = event["taskBucket"]
    task_info = event['taskInfo']

    # response = s3_client.get_object(Bucket=task_bucket, Key=task_code)
    # contents = response['Body'].read()
    subprocess.call(["aws", "-l"])



    # time_in_secs = (time.time() - start_time)
    #
    # metadata = {
    #     "processingtime": '%s' % time_in_secs,
    #     "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # }
    # print("metadata: ", metadata)

    return json.dump("sdsad")