from moto import mock_s3
import json
from localstack.utils.aws import aws_stack


@mock_s3
def test_s3():
    s3_client = aws_stack.connect_to_service('s3')
    # s3_client.create_bucket(Bucket=TEST_BUCKET_NAME_WITH_POLICY)
    # s3_endpoint_url = os.environ.get('s3_endpoint_url')
    # s3 = boto3.resource('s3')
    # if s3_endpoint_url:
    #     s3_client = boto3.client('s3', endpoint_url=s3_endpoint_url)
    # else:
    # s3_client = boto3.client('s3')

    key = 'test'
    data = json.dumps({
            "mapCount": 10,
            "totalS3Files": 20,
            "startTime": 100
        })
    s3_client.create_bucket(Bucket="serverless-mapreduce-storage")
    s3_client.put_object(Bucket="serverless-mapreduce-storage", Key=key, Body=data, Metadata={})
    response = s3_client.get_object(Bucket="serverless-mapreduce-storage", Key=key)
    content = response['Body'].read()
    print(content)


if __name__ == "__main__":
    test_s3()
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