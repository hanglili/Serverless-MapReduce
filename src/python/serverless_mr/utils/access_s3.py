import boto3

from serverless_mr.static.static_variables import StaticVariables


def write_to_s3(bucket, key, data, metadata):
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION, endpoint_url='http://localhost:4572')
    # s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)


# def write_job_config(job_id, job_bucket, num_mappers, reducer_function, reduce_handler, reduce_count):
#     filename = StaticVariables.JOB_INFO_LAMBDA_PATH
#     with open(filename, 'w') as f:
#         data = json.dumps({
#             # "jobId": job_id,
#             # "jobBucket": job_bucket,
#             "mapCount": num_mappers,
#             # "reducerFunction": reducer_function,
#             # "reducerHandler": reduce_handler,
#             # "reduceCount": reduce_count
#         }, indent=4)
#         f.write(data)
