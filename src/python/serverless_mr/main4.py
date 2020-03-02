from serverless_mr.aws_lambda import lambda_manager
import boto3
from serverless_mr.utils import zip
import json
from botocore.client import Config
import os


def test_s3():
    s3_endpoint_url = os.environ.get('s3_endpoint_url')
    s3 = boto3.resource('s3')
    # if s3_endpoint_url:
    #     s3_client = boto3.client('s3', endpoint_url=s3_endpoint_url)
    # else:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    s3 = boto3.resource('s3',  endpoint_url='http://localhost:4572')

    key = 'testKey'
    data = json.dumps({
        "mapCount": 10,
        "totalS3Files": 20,
        "startTime": 100
    })
    bucket_name = "serverless-mapreduce-storage"
    # s3_client.create_bucket(Bucket=bucket_name)
    s3.create_bucket(Bucket=bucket_name)
    s3.Bucket(bucket_name).put_object(Key=key, Body=data, Metadata={})
    # s3_client.put_object(Bucket=bucket_name, Key=key, Body=data, Metadata={})
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read()
    print(content)


def test_lambda():
    # init
    bucket = "serverless-mapreduce-storage"
    region = "us-east-1"
    map_zip_name = "map.zip"
    lambda_name = "mapper5"
    job_id = "job"
    lambda_memory = 1536
    concurrent_lambdas = 1000
    lambda_read_timeout = 300
    boto_max_connections = 1000
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    lambda_config = Config(read_timeout=lambda_read_timeout,
                           max_pool_connections=boto_max_connections,
                           region_name=region)
    lambda_client = boto3.client('lambda', endpoint_url='http://localhost:4574', config=lambda_config)

    zip.zip_lambda("job/map.py", map_zip_name)
    l_mapper = lambda_manager.LambdaManager(lambda_client, s3_client, region,
                                            map_zip_name, job_id,
                                            lambda_name, "job/map.map_function")
    l_mapper.update_code_or_create_on_no_exist()

    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'Events': ['s3:ObjectCreated:*'],
                    'LambdaFunctionArn': l_mapper.function_arn,
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': '/'
                                },
                            ]
                        }
                    }
                }
            ],
            # 'TopicConfigurations' : [],
            # 'QueueConfigurations' : []
        }
    )

    # l_mapper.create_s3_event_source_notification(bucket_name, '/')

    key = 'testKey3'
    data = json.dumps({
        "mapCount": 10,
        "totalS3Files": 20,
        "startTime": 200
    })
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata={})
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "bucket": "serverless-mapreduce-storage",
            "keys": "testKey",
            "jobBucket": "serverless-mapreduce-storage",
            "jobId": "job",
            "mapperId": 1
        })
    )
    print(response)
    content = response['Payload'].read()
    print("The final contents are:", content)


if __name__ == "__main__":
    test_s3()
    # test_lambda()
    # if len(sys.argv) < 2:
    #     print("Wrong number of arguments.")
    # else:
    #     mode = sys.argv[1]
    #
    #     if int(mode) == 0:
    #         driver = Driver()
    #         driver.run()
    #     else:
    #         serverless_driver_setup = ServerlessDriverSetup()
    #         serverless_driver_setup.register_driver()
    #         print("Driver Lambda function successfully registered")
    #         command = input("Enter invoke to invoke and other keys to exit: ")
    #         if command == "invoke":
    #             print("Driver invoked and starting job execution")
    #             serverless_driver_setup.invoke()
