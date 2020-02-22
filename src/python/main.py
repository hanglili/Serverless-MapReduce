import sys
import boto3
import json

from driver.driver import Driver
from driver.serverless_driver_setup import ServerlessDriverSetup
from static.static_variables import StaticVariables


def set_up_input_data():
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    config = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, "r").read())
    input_bucket = config["bucket"]
    prefix = config["prefix"]
    # job_bucket = config["jobBucket"]
    s3_client.create_bucket(Bucket=input_bucket)
    s3_client.put_bucket_acl(
        ACL='public-read-write',
        Bucket=input_bucket,
    )

    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-1',
                          Bucket=input_bucket, Key='testing_partitioned/input-1')
    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-2',
                          Bucket=input_bucket, Key='testing_partitioned/input-2')
    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-4',
                          Bucket=input_bucket, Key='testing_partitioned/input-4')


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Wrong number of arguments.")
    else:
        set_up_input_data()
        mode = sys.argv[1]

        if int(mode) == 0:
            driver = Driver()
            driver.run()
        else:
            serverless_driver_setup = ServerlessDriverSetup()
            serverless_driver_setup.register_driver()
            print("Driver Lambda function successfully registered")
            command = input("Enter invoke to invoke and other keys to exit: ")
            if command == "invoke":
                print("Driver invoked and starting job execution")
                serverless_driver_setup.invoke()
