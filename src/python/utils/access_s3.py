import boto3
import json

from aws_xray_sdk.core import xray_recorder

JOB_INFO = 'configuration/job-info.json'


@xray_recorder.capture('write_to_s3')
def write_to_s3(bucket, key, data, metadata):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


@xray_recorder.capture('write_job_config')
def write_job_config(job_id, job_bucket, n_mappers, r_func, r_handler, reduce_count):
    fname = JOB_INFO
    with open(fname, 'w') as f:
        data = json.dumps({
            "jobId": job_id,
            "jobBucket": job_bucket,
            "mapCount": n_mappers,
            "reducerFunction": r_func,
            "reducerHandler": r_handler,
            "reduceCount": reduce_count
        }, indent=4)
        f.write(data)
