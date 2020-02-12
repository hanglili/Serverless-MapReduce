import json
import time


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    start_time = time.time()

    # Job Bucket. We just got a notification from this bucket
    bucket = event['Records'][0]['s3']['bucket']['name']

    # key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    config = json.loads(open(JOB_INFO, "r").read())

    job_id = config["jobId"]
    map_count = config["mapCount"]
    r_function_name = config["reducerFunction"]
    r_handler = config["reducerHandler"]

    config = json.loads(open(JOB_INFO, "r").read())
    num_reducers = config["reduceCount"]