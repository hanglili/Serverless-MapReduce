import boto3
import json

from serverless_mr.static.static_variables import StaticVariables


static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
    local_endpoint_url = 'http://localhost:4569'
    gateway_client = boto3.client('apigateway', aws_access_key_id='', aws_secret_access_key='',
                          region_name=StaticVariables.DEFAULT_REGION,
                          endpoint_url=local_endpoint_url)
else:
    gateway_client = boto3.client('apigateway')



