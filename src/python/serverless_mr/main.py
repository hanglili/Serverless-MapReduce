import sys
import boto3
import json
import importlib.resources
import os
import shutil

from pathlib import Path
from serverless_mr.driver.driver import Driver
from serverless_mr.driver.serverless_driver_setup import ServerlessDriverSetup
from serverless_mr.static.static_variables import StaticVariables


project_working_dir = os.getcwd()
library_dir = Path(os.path.dirname(os.path.realpath(__file__)))
library_working_dir = library_dir.parent


def find_filepath(package_name, filename):
    with importlib.resources.path(package_name, filename) as path:
        return str(path)


def delete_dir(dirname):
    dst_dir = "%s/%s" % (library_dir, dirname)
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)


def delete_files(dirname, filenames):
    for filename in filenames:
        dst_file = "%s/%s/%s" % (library_dir, dirname, filename)
        if os.path.exists(dst_file):
            os.remove(dst_file)


def copy_files(dirname, dst_dirname, filenames):
    dst_dir = "%s/%s" % (library_dir, dst_dirname)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    for filename in filenames:
        path = find_filepath(dirname, filename)
        if os.path.normpath(path) != os.path.normpath(dst_dir + "/" + filename):
            shutil.copy2(path, dst_dir)


def set_up():
    os.chdir(library_working_dir)
    config_dirname = "configuration"
    copy_files(config_dirname, config_dirname, ["static-job-info.json", "driver.json"])
    copy_files("user_job", "job", ["map.py", "reduce.py", "partition.py"])


def tear_down():
    config_dirname = "configuration"
    delete_dir(config_dirname)
    delete_files("job", ["map.py", "reduce.py", "partition.py"])


def set_up_input_data(config):
    print("Setting up input data")
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    input_bucket = config[StaticVariables.INPUT_SOURCE_FN]
    prefix = config[StaticVariables.INPUT_PREFIX_FN]

    s3_client.create_bucket(Bucket=input_bucket)
    s3_client.put_bucket_acl(
        ACL='public-read-write',
        Bucket=input_bucket,
    )

    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-1',
                          Bucket=input_bucket, Key='%sinput-1' % prefix)
    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-2',
                          Bucket=input_bucket, Key='%sinput-2' % prefix)
    s3_client.upload_file(Filename='../../input_data/testing_partitioned/input-4',
                          Bucket=input_bucket, Key='%sinput-4' % prefix)
    print("Finished setting up input data")


def set_up_job_bucket(config):
    print("Setting up job bucket")
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    shuffling_bucket = config[StaticVariables.SHUFFLING_BUCKET_FN]
    # TODO: Check if the bucket exists first
    s3_client.create_bucket(Bucket=shuffling_bucket)
    s3_client.put_bucket_acl(
        ACL='public-read-write',
        Bucket=shuffling_bucket,
    )


def create_dynamo_table(client, table_name):
    response = client.create_table(
        AttributeDefinitions=[{
            'AttributeName': 'id',
            'AttributeType': 'N'
        }],
        TableName=table_name,
        KeySchema=[{
            'AttributeName': 'id',
            'KeyType': 'HASH'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    # print(response)
    # print(json.dumps(response))


def put_items(client, table_name, filepath):
    with open(filepath) as fp:
        line = fp.readline()
        cnt = 1
        while line:
            response = client.put_item(
                TableName=table_name,
                Item={
                    'id': {'N': str(cnt)},
                    'line': {'S': line.strip()}
                }
            )
            line = fp.readline()
            cnt += 1


# DynamoDB table is config["bucket"]?
def set_up_input_data(config):
    print("Setting up input data")
    client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4569')
    prefix = config[StaticVariables.INPUT_PREFIX_FN]
    create_dynamo_table(client, '%sinput-1' % prefix)
    create_dynamo_table(client, '%sinput-2' % prefix)
    create_dynamo_table(client, '%sinput-4' % prefix)
    put_items(client, ('%sinput-1' % prefix), '../../input_data/testing_partitioned/input-5')
    put_items(client, ('%sinput-2' % prefix), '../../input_data/testing_partitioned/input-6')
    # put_items(client, ('%sinput-4' % prefix), '../../input_data/testing_partitioned/input-4')
    print("Finished setting up input data")
    response = client.get_item(
        Key={
            'id': {'N': '1'}
        },
        TableName=('%sinput-2' % prefix)
    )
    print(response['Item'])


def init_job(args):
    if len(args) < 2:
        print("Wrong number of arguments.")
    else:
        set_up()
        static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
        static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()

        set_up_job_bucket(static_job_info)
        if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            os.chdir(project_working_dir)
            set_up_input_data(static_job_info)
            os.chdir(library_working_dir)

        mode = args[1]
        print("The mode of run is", mode)

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

        tear_down()


if __name__ == "__main__":
    init_job(sys.argv)

