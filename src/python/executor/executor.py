import boto3
import json
import os
import subprocess
import resource
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def download_directory_from_s3(bucket_name, remote_directory_name):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=remote_directory_name):
        if not obj.key.endswith("/"):
            print("The object is", obj)
            if not os.path.exists("/tmp/" + os.path.dirname(obj.key)):
                os.makedirs("/tmp/" + os.path.dirname(obj.key))
            print("The filepath is", obj.key)
            bucket.download_file(obj.key, "/tmp/")


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # start_time = time.time()

    task_code = event['taskCode']
    task_bucket = event["taskBucket"]
    task_info = event['taskInfo']

    # response = s3_client.get_object(Bucket=task_bucket, Key=task_code)
    # contents = response['Body'].read()

    download_directory_from_s3(task_bucket, task_code)
    # bucket = s3.Bucket(task_bucket)
    # bucket.download_file(task_code, "/tmp/" + task_code)
    subprocess.call(["ls", "/tmp/"])

    # time_in_secs = (time.time() - start_time)
    #
    # metadata = {
    #     "processingtime": '%s' % time_in_secs,
    #     "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # }
    # print("metadata: ", metadata)

    return json.dump("sdsad")