import boto3
import json

local_endpoint_url = 'http://localhost:4572'
s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                        region_name="us-east-1",
                                        endpoint_url=local_endpoint_url)

outputs = [1, 2, 3]

s3_client.put_object(Bucket="serverless-mapreduce-storage",
                     Key="bl-release/stage-3/3", Body=json.dumps(outputs))
