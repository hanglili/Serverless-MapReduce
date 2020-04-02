import boto3
import json
import importlib.resources
import os
import shutil
import glob

from pathlib import Path
from serverless_mr.driver.driver import Driver
from serverless_mr.driver.serverless_driver_setup import ServerlessDriverSetup
from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import input_handler

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


def set_up_shuffling_bucket(static_job_info):
    print("Setting up shuffling bucket")
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='us-east-1',
                             endpoint_url='http://localhost:4572')
    shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
    s3_client.create_bucket(Bucket=shuffling_bucket)
    s3_client.put_bucket_acl(
        ACL='public-read-write',
        Bucket=shuffling_bucket,
    )
    print("Finished setting up shuffling bucket")


def init_job():
    set_up()
    static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
    static_job_info = json.loads(static_job_info_file.read())
    static_job_info_file.close()

    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
        set_up_shuffling_bucket(static_job_info)
        cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN])

        os.chdir(project_working_dir)
        local_testing_input_path = static_job_info[StaticVariables.LOCAL_TESTING_INPUT_PATH]
        local_file_paths = glob.glob(local_testing_input_path + "*")
        print(local_file_paths)
        cur_input_handler.set_up_local_input_data(local_file_paths)
        os.chdir(library_working_dir)

    is_serverless_driver = static_job_info[StaticVariables.SERVERLESS_DRIVER_FLAG_FN]

    if is_serverless_driver:
        serverless_driver_setup = ServerlessDriverSetup()
        serverless_driver_setup.register_driver()
        print("Driver Lambda function successfully registered")
        command = input("Enter invoke to invoke and other keys to exit: ")
        if command == "invoke":
            print("Driver invoked and starting job execution")
            serverless_driver_setup.invoke()
    else:
        driver = Driver()
        driver.run()

    tear_down()


if __name__ == "__main__":
    init_job()

