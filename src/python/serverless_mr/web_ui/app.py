import json
import boto3
import os
import sys
import subprocess

from pathlib import Path

project_working_dir = os.getcwd()
library_dir = Path(os.path.dirname(os.path.realpath(__file__)))
library_working_dir = library_dir.parent
sys.path.insert(0, str(library_working_dir))
for path in sys.path:
    print("Sys path:", path)


from datetime import datetime
from flask import Flask, jsonify, render_template, request, url_for, send_from_directory
from flask_cors import CORS, cross_origin
from botocore.client import Config
from utils import in_degree, stage_state, stage_progress
from static.static_variables import StaticVariables
from data_sources import input_handler_s3


# app = Flask(__name__, static_folder='./templates/public', template_folder="./templates/static")
app = Flask(__name__, template_folder="./templates/static")
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
# app.config["APPLICATION_ROOT"] = "/dev"


def is_production():
    root_url = request.url_root
    developer_url = 'http://127.0.0.1:5000/'
    localhost_url = 'http://localhost:5000/'
    return root_url != developer_url and root_url != localhost_url


@app.route('/hello-world')
@cross_origin()
def hello_world():
    return jsonify('ServerlessMR Web Application')


@app.route("/url")
@cross_origin()
def get_url():
    return "The URL for this page is {}".format(url_for("/"))


def delete_files(dirname, filenames):
    print("At delete_files, the current working directory is", os.getcwd())
    for filename in filenames:
        if dirname == "":
            dst_file = filename
        else:
            dst_file = "%s/%s" % (dirname, filename)
        print("The file to delete is", dst_file)
        if os.path.exists(dst_file):
            os.remove(dst_file)


@app.route("/register-job", methods=['POST'])
@cross_origin()
def register_job():
    print("WebUI: Received request for path /register-job")
    print("Current working directory is", os.getcwd())
    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    if is_local_testing:
        local_endpoint_url = 'http://localhost:4572'
        client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                              region_name=StaticVariables.DEFAULT_REGION,
                              endpoint_url=local_endpoint_url)
        TMP_DIR_NAME = 'web_ui/tmp'
        os.chdir(library_working_dir)
    else:
        client = boto3.client('s3')
        TMP_DIR_NAME = '/tmp'
        bucket_name = 'serverless-mr-code'
        # 1. Download the S3 code files to /tmp including the user-provided code files.
        contents = client.list_objects(Bucket=bucket_name).get("Contents", [])
        for content in contents:
            key = content.get('Key')
            print("Current key:", key)
            if not key.endswith("/"):
                dest_pathname = os.path.join(TMP_DIR_NAME, key)
                print("Current destination path:", dest_pathname)
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
                client.download_file(Bucket=bucket_name, Key=key, Filename=dest_pathname)

    content = request.form.to_dict()

    static_job_info = content['static-job-info.json']
    dest_pathname = os.path.join(TMP_DIR_NAME, 'configuration/static-job-info.json')
    with open(dest_pathname, 'w') as f:
        f.write(static_job_info)
    driver_config = content['driver.json']
    dest_pathname = os.path.join(TMP_DIR_NAME, 'configuration/driver.json')
    with open(dest_pathname, 'w') as f:
        f.write(driver_config)

    user_main = content['user_main.py']
    dest_pathname = os.path.join(TMP_DIR_NAME, 'user_main.py')
    with open(dest_pathname, 'w') as f:
        f.write(user_main)

    functions = content['functions.py']
    dest_pathname = os.path.join(TMP_DIR_NAME, 'user_functions/functions.py')
    with open(dest_pathname, 'w') as f:
        f.write(functions)

    # 2. Set working directory to /tmp or tmp depending on whether it is a local execution.
    os.chdir(TMP_DIR_NAME)

    # 3. Run user_main.py
    my_env = os.environ.copy()
    return_code = subprocess.call(
        ["python3.7", "user_main.py"], env=my_env
    )

    if is_local_testing:
        delete_files("user_functions", ["functions.py"])
        delete_files("configuration", ["static-job-info.json", "driver.json"])
        delete_files("", ["user_main.py"])
        os.chdir(project_working_dir)
    if return_code == 0:
        return "Successfully registered"
    else:
        return "Error"


# @app.route('/public/bundle.js')
# def custom_static():
#     return send_from_directory('templates/public', 'bundle.js')


@app.route('/public/<path:filename>')
@cross_origin()
def online_custom_static(filename):
    print("WebUI: Received request for path /public/%s" % filename)
    return send_from_directory('templates/public', filename)


@app.route('/dev/public/<path:filename>')
@cross_origin()
def local_custom_static(filename):
    print("WebUI: Received request for path /dev/public/%s" % filename)
    return send_from_directory('templates/public', filename)


@app.route('/')
@app.route('/index')
@app.route('/dev/table')
@cross_origin()
def index():
    print("WebUI: Received request for path /")
    if is_production():
        return render_template("index.html")
    else:
        return render_template("index_local.html")


@app.route('/username')
@cross_origin()
def get_username():
    print("WebUI: Received request for path /username")
    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    if is_local_testing:
        local_endpoint_url = 'http://localhost:4592'
        client = boto3.client('sts', aws_access_key_id='', aws_secret_access_key='',
                              region_name=StaticVariables.DEFAULT_REGION,
                              endpoint_url=local_endpoint_url)
    else:
        client = boto3.client('sts')
    return client.get_caller_identity().get('Account')


@app.route("/jobs", methods=['GET'])
@cross_origin()
def get_jobs_info():
    print("WebUI: Received request for path /jobs")
    print("Current working directory is", os.getcwd())
    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    if is_local_testing:
        local_endpoint_url = 'http://localhost:4572'
        client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                              region_name=StaticVariables.DEFAULT_REGION,
                              endpoint_url=local_endpoint_url)
    else:
        client = boto3.client('s3')
    job_keys_list = []
    prefix = "web-ui/"
    for obj in client.list_objects(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                   Prefix=prefix)["Contents"]:
        if not obj["Key"].endswith('/') and \
                ("static-job-info.json" in obj["Key"] or "registered-job-info.json" in obj["Key"]):
            job_keys_list.append(obj["Key"])

    jobs_information = {}
    completed = []
    active = []
    registered = []
    for job_key in job_keys_list:
        response = client.get_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                     Key=job_key)
        contents = response['Body'].read()
        cur_job_info = json.loads(contents)
        if "static-job-info.json" in job_key:
            if cur_job_info["completed"]:
                completed.append(cur_job_info)
            else:
                submission_time = datetime.strptime(cur_job_info["submissionTime"], "%Y-%m-%d_%H.%M.%S")
                duration = datetime.utcnow() - submission_time
                cur_job_info['duration'] = str(duration).split(".")[0]
                active.append(cur_job_info)
        else:
            registered.append(cur_job_info)

    jobs_information["completed"] = completed
    jobs_information["active"] = active
    jobs_information["registered"] = registered
    return jsonify(jobs_information)


@app.route("/invoke-job", methods=['GET'])
@cross_origin()
def invoke_job():
    job_name = request.args.get('job-name')
    driver_lambda_name = request.args.get('driver-lambda-name')
    print("WebUI: Received request for path /invoke-job with parameters", job_name, driver_lambda_name)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    if is_local_testing:
        local_endpoint_url = 'http://localhost:4572'
        client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                              region_name=StaticVariables.DEFAULT_REGION,
                              endpoint_url=local_endpoint_url)
    else:
        client = boto3.client('s3')
    s3_driver_config_key = StaticVariables.S3_UI_REGISTERED_JOB_DRIVER_CONFIG_PATH % job_name
    response = client.get_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                 Key=s3_driver_config_key)
    contents = response['Body'].read()
    config = json.loads(contents)
    region = config[StaticVariables.REGION_FN] \
        if StaticVariables.REGION_FN in config else StaticVariables.DEFAULT_REGION
    lambda_read_timeout = config[StaticVariables.LAMBDA_READ_TIMEOUT_FN] \
        if StaticVariables.LAMBDA_READ_TIMEOUT_FN in config else StaticVariables.DEFAULT_LAMBDA_READ_TIMEOUT
    boto_max_connections = config[StaticVariables.BOTO_MAX_CONNECTIONS_FN] \
        if StaticVariables.BOTO_MAX_CONNECTIONS_FN in config else StaticVariables.DEFAULT_BOTO_MAX_CONNECTIONS

    # Setting longer timeout for reading aws_lambda results and larger connections pool
    lambda_config = Config(read_timeout=lambda_read_timeout,
                           max_pool_connections=boto_max_connections,
                           region_name=region)
    if is_local_testing:
        lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                     region_name=region,
                                     endpoint_url='http://localhost:4574', config=lambda_config)
    else:
        lambda_client = boto3.client('lambda', config=lambda_config)
    response = lambda_client.invoke(
        FunctionName=driver_lambda_name,
        InvocationType='Event',
        Payload=json.dumps({})
    )
    print(response)
    return jsonify(response['ResponseMetadata'])


@app.route("/schedule-job", methods=['GET'])
@cross_origin()
def schedule_job():
    job_name = request.args.get('job-name')
    driver_lambda_name = request.args.get('driver-lambda-name')
    schedule_expression = request.args.get('schedule-expression')
    print("WebUI: Received request for path /schedule-job with parameters", job_name, driver_lambda_name, schedule_expression)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    if is_local_testing:
        # local_endpoint_url = 'http://localhost:4586'
        # client = boto3.client('events',
        #                       region_name=StaticVariables.DEFAULT_REGION,
        #                       endpoint_url=local_endpoint_url)
        from localstack.utils.aws import aws_stack
        client = aws_stack.connect_to_service('events')
    else:
        client = boto3.client('events')

    # Put an event rule
    response = client.put_rule(
        Name='%s-scheduling-rule' % job_name,
        # EventPattern=json.dumps({'Hello': 'hello'}),
        RoleArn=os.environ.get("serverless_mapreduce_role"),
        # ScheduleExpression='rate(30 minutes)',
        # ScheduleExpression='cron(35 11 * * ? *)',
        ScheduleExpression=schedule_expression,
        # ScheduleExpression='rate(5 minute)',
        State='ENABLED'
    )
    print(response['RuleArn'])

    response = client.put_targets(
        Rule='%s-scheduling-rule' % job_name,
        Targets=[
            {
                'Arn': driver_lambda_name,
                'Id': 'myCloudWatchEventsTarget',
            }
        ]
    )
    print(response)
    return jsonify(response)


@app.route("/in-degree", methods=['GET'])
@cross_origin()
def get_in_degree_info():
    job_name = request.args.get('job-name')
    submission_time = request.args.get('submission-time')
    print("WebUI: Received request for path /in-degree with parameters", job_name, submission_time)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    in_degree_obj = in_degree.InDegree(in_lambda=False, is_local_testing=is_local_testing)
    in_degrees = in_degree_obj.read_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME
                                                    % (job_name, submission_time))
    return jsonify(in_degrees)


@app.route("/stage-progress", methods=['GET'])
@cross_origin()
def get_stage_progress():
    job_name = request.args.get('job-name')
    submission_time = request.args.get('submission-time')
    print("WebUI: Received request for path /stage-progress with parameters", job_name, submission_time)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    stage_progress_obj = stage_progress.StageProgress(in_lambda=False, is_local_testing=is_local_testing)
    stages_progress = stage_progress_obj.read_progress_table(StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME
                                                             % (job_name, submission_time))
    return jsonify(stages_progress)


@app.route("/num-completed-operators", methods=['GET'])
@cross_origin()
def get_num_completed_operators():
    job_name = request.args.get('job-name')
    submission_time = request.args.get('submission-time')
    print("WebUI: Received request for path /num-completed-operators with parameters", job_name, submission_time)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False, is_local_testing=is_local_testing)
    s3_stage_conf_path = StaticVariables.S3_UI_STAGE_CONFIGURATION_PATH % (job_name, submission_time)
    stage_config = json.loads(input_handler_s3_obj.read_records_from_input_key(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                                                               s3_stage_conf_path, None))

    stage_state_obj = stage_state.StageState(in_lambda=False, is_local_testing=is_local_testing)
    stage_states = stage_state_obj.read_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME
                                                    % (job_name, submission_time))
    result = {}
    for stage_id, stage_num_completed in stage_states.items():
        result[stage_id] = [stage_num_completed, stage_config[stage_id]["num_operators"]]
    return jsonify(result)


@app.route("/dag", methods=['GET'])
@cross_origin()
def get_dag_information():
    job_name = request.args.get('job-name')
    submission_time = request.args.get('submission-time')
    print("WebUI: Received request for path /dag with parameters", job_name, submission_time)

    is_local_testing = os.environ.get("local_testing") == 'True' or os.environ.get("local_testing") == 'true'
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False, is_local_testing=is_local_testing)
    s3_dag_information_path = StaticVariables.S3_UI_DAG_INFORMATION_PATH % (job_name, submission_time)
    dag_data = json.loads(input_handler_s3_obj.read_records_from_input_key(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                                                           s3_dag_information_path, None))
    return jsonify(dag_data)


def start_web_ui():
    app.run()


if __name__ == '__main__':
    app.run()
