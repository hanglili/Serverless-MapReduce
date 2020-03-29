import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables

# create an S3 and Lambda session
static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info['localTesting']:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
    lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                 region_name=StaticVariables.DEFAULT_REGION,
                                 endpoint_url='http://%s:4574' % os.environ['LOCALSTACK_HOSTNAME'])
else:
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')


def get_mapper_files(num_bins, bucket, job_id):
    bins_of_keys = [[] for _ in range(num_bins + 1)]

    for i in range(1, num_bins + 1):
        prefix = "%s/%sbin%s/" % (job_id, StaticVariables.MAP_OUTPUT_PREFIX, i)
        files = s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]
        bins_of_keys[i] = files

    return bins_of_keys


def lambda_handler(event, _):
    print("Received event: " + json.dumps(event, indent=2))

    # start_time = time.time()

    # Job Bucket. We just got a notification from this bucket
    job_bucket = event['Records'][0]['s3']['bucket']['name']

    # key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    job_id = static_job_info["jobId"]
    reduce_function_name = static_job_info["reducerFunction"]
    output_bucket = static_job_info['outputBucket']
    output_prefix = static_job_info['outputPrefix']
    # reduce_handler = static_job_info["reducerHandler"]
    num_reducers = static_job_info["reduceCount"]
    use_combine = static_job_info["useCombine"]

    map_count = int(os.environ.get("num_mappers"))

    prefix = "%s/%sbin%s/" % (job_id, StaticVariables.MAP_OUTPUT_PREFIX, str(num_reducers))

    # Get Mapper Finished Count
    # Get job files
    files = s3_client.list_objects(Bucket=job_bucket, Prefix=prefix)["Contents"]

    # Stateless Coordinator logic
    num_finished_mappers = len(files)
    print("Number of mappers completed: ", num_finished_mappers)

    if map_count == num_finished_mappers:

        # All the mappers have finished, time to schedule the reducers
        bins_of_keys = get_mapper_files(num_reducers, job_bucket, job_id)

        for i in range(1, num_reducers + 1):
            cur_reducer_keys = [b['Key'] for b in bins_of_keys[i]]
            print("The reduce function name is", reduce_function_name)
            # invoke the reducers asynchronously
            response = lambda_client.invoke(
                FunctionName=reduce_function_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "bucket": job_bucket,
                    "outputBucket": output_bucket,
                    "outputPrefix": output_prefix,
                    "keys": cur_reducer_keys,
                    "jobBucket": job_bucket,
                    "jobId": job_id,
                    "useCombine": use_combine,
                    # "numReducers": num_reducers,
                    # "stepId": step_id,
                    "reducerId": i
                })
            )
            print("Reducer invocation response: ", response)

        print("Finished scheduling %s number of reducers" % num_reducers)
    else:
        print("Still waiting for all the mappers to finish ...")


'''
ev = {
    "Records": [{'s3': {'bucket': {'name': "smallya-useast-1"}}}],
    "bucket": "smallya-useast-1",
    "jobId": "jobid134",
    "mapCount": 1,
    "reducerFunctionName": "shell-exec",
    "reducerHandler": "index.handler"
}
lambda_handler(ev, {})
'''