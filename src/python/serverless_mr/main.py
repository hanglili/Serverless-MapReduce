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


def set_up_input_data(config):
    print("Setting up input data")
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    input_bucket = config["bucket"]
    prefix = config["prefix"]
    # job_bucket = config["jobBucket"]
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


def init_job(args):
    if len(args) < 2:
        print("Wrong number of arguments.")
    else:
        os.chdir(library_working_dir)
        print("The current working directory is", os.getcwd())
        config_dirname = "configuration"
        copy_files(config_dirname, config_dirname, ["static-job-info.json", "driver.json"])
        copy_files("user_job", "job", ["map.py", "reduce.py"])
        config = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, "r").read())
        if config['localTesting']:
            os.chdir(project_working_dir)
            set_up_input_data(config)
            os.chdir(library_working_dir)
        mode = args[1]
        print(mode)

        print("The current working directory is", os.getcwd())

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


        delete_dir(config_dirname)
        delete_files("job", ["map.py", "reduce.py"])


if __name__ == "__main__":
    init_job(sys.argv)

