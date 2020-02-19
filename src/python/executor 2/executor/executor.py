import boto3
import json
import os
import subprocess
import zipfile
import resource
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

TMP_DIR_NAME = "/tmp"
TASK_CODE_ZIP_NAME = "/task_code.zip"
TASK_CODE_DIR_NAME = "/task_code"


def download_directory_from_s3(bucket_name, remote_directory_name):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=remote_directory_name):
        if not obj.key.endswith("/"):
            print("The object is", obj)
            if not os.path.exists("/tmp/" + os.path.dirname(obj.key)):
                os.makedirs("/tmp/" + os.path.dirname(obj.key))
            print("The filepath is", obj.key)
            bucket.download_file(obj.key, "%s%s" % (TMP_DIR_NAME, TASK_CODE_ZIP_NAME))


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
    subprocess.call(["ls", TMP_DIR_NAME])

    with zipfile.ZipFile("%s%s" % (TMP_DIR_NAME, TASK_CODE_ZIP_NAME), 'r') as zip_ref:
        zip_ref.extractall("%s%s" % (TMP_DIR_NAME, TASK_CODE_DIR_NAME))

    subprocess.call(["ls", TMP_DIR_NAME])
    # subprocess.call(["cd", TMP_DIR_NAME])
    # subprocess.call(["cd", TASK_CODE_DIR_NAME])
    os.chdir("%s%s" % (TMP_DIR_NAME, TASK_CODE_DIR_NAME))
    arguments = "(%s, %s)" % (task_info, context)
    subprocess.call(["python", "-c",
                     "from job.map_handler import map_handler; "
                     "from job.map import map; "
                     "print(map_handler(map)(sys.argv[1], sys.argv[2]))",
                     str(task_info), str(context)])
    # subprocess.call(["python3", "-c",
    #                  "from job.partition import partition; "
    #                  "import boto3; "
    #                  "print(partition(\"3123131\", 1))"])

    # time_in_secs = (time.time() - start_time)
    #
    # metadata = {
    #     "processingtime": '%s' % time_in_secs,
    #     "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # }
    # print("metadata: ", metadata)

    return json.dumps("The task has been completed successfully")
