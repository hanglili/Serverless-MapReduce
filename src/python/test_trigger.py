import boto3
import json

local_endpoint_url = 'http://localhost:4572'
s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                         region_name="us-east-1",
                         endpoint_url=local_endpoint_url)

outputs = [1, 2, 3]

def set_up_shuffling_input_bucket(shuffling_input_bucket):
    print("Setting up shuffling input bucket")
    s3_client.create_bucket(Bucket=shuffling_input_bucket)
    s3_client.put_bucket_acl(
        ACL='public-read-write',
        Bucket=shuffling_input_bucket,
    )
    print("Finished setting up shuffling bucket")

shuffling_bucket_name = "serverless-mapreduce-storage"
set_up_shuffling_input_bucket(shuffling_bucket_name)
s3_client.put_object(Bucket=shuffling_bucket_name,
                     Key="bl-release/stage-3/3", Body=json.dumps(outputs))

# outputs = [4, 5, 6]
# s3_client.put_object(Bucket=shuffling_bucket_name,
#                      Key="bl-release/stage-3/3", Body=json.dumps(outputs))
