from moto import mock_s3
from moto import mock_lambda
import boto3
import json
import base64
import os


@mock_s3
def test_s3():
    s3_endpoint_url = os.environ.get('s3_endpoint_url')
    s3 = boto3.resource('s3')
    # if s3_endpoint_url:
    #     s3_client = boto3.client('s3', endpoint_url=s3_endpoint_url)
    # else:
    s3_client = boto3.client('s3')

    key = 'test'
    data = json.dumps({
            "mapCount": 10,
            "totalS3Files": 20,
            "startTime": 100
        })
    s3_client.create_bucket(Bucket="serverless-mapreduce-storage")
    s3.Bucket("serverless-mapreduce-storage").put_object(Key=key, Body=data, Metadata={})
    response = s3_client.get_object(Bucket="serverless-mapreduce-storage", Key=key)
    content = response['Body'].read()
    print(content)


@mock_lambda
def test_lambda():
    conn = boto3.client("lambda", "us-east-1", endpoint_url='http://127.0.0.1:5000')
    conn.create_function(
        FunctionName="testFunction",
        Runtime="python3.7",
        Role="arn:aws:iam::990092034516:role/serverless_mr_role",
        Handler="job/map.map_function",
        Code={"ZipFile": open("map.zip", 'rb').read()},
        Description="test lambda function",
        Timeout=3,
        MemorySize=128,
        Publish=True,
    )
    print("Hello")

    in_data = {"msg": "So long and thanks for all the fish"}
    success_result = conn.invoke(
        FunctionName="testFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps(in_data),
    )
    print("Hello2")
    success_result["StatusCode"].should.equal(200)
    result_obj = json.loads(
        base64.b64decode(success_result["LogResult"]).decode("utf-8")
    )

    result_obj.should.equal(in_data)

    payload = success_result["Payload"].read().decode("utf-8")
    json.loads(payload).should.equal(in_data)
    # init
    # bucket = "serverless-mapreduce-storage"
    # region = "us-east-1"
    # lambda_memory = 1536
    # concurrent_lambdas = 1000
    # lambda_read_timeout = 300
    # boto_max_connections = 1000
    # s3_client = boto3.client('s3')
    # lambda_config = Config(read_timeout=lambda_read_timeout,
    #                             max_pool_connections=boto_max_connections,
    #                             region_name=region)
    # lambda_client = boto3.client('lambda', region)
    #
    # zip.zip_lambda("job/map.py", "map.zip")
    # l_mapper = lambda_manager.LambdaManager(lambda_client, s3_client, region,
    #                                         "map.zip", "job",
    #                                         "hang", "job/map.map_function")
    # l_mapper.update_code_or_create_on_no_exist()
    # resp = lambda_client.invoke(
    #     FunctionName="hang",
    #     InvocationType='RequestResponse',
    #     Payload=json.dumps({
    #         "bucket": "dasdsa",
    #         # "keys": "dasdsa",
    #         # "jobBucket": "dasdsa",
    #         # "jobId": "dasdsa",
    #         # "mapperId": 2
    #     })
    # )
    # content = response['Body'].read()
    # print(content)


if __name__ == "__main__":
    test_lambda()
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
