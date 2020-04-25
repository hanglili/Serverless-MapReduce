import json
import boto3
import os


# from localstack.utils.aws import aws_stack
from datetime import datetime
from flask import Flask, jsonify, render_template, request, url_for, send_from_directory
from flask_cors import CORS, cross_origin
# from serverless_mr.utils import in_degree, stage_state, stage_progress
# from serverless_mr.static.static_variables import StaticVariables
# from serverless_mr.data_sources import input_handler_s3
# from botocore.client import Config


# app = Flask(__name__, static_folder='./templates/public', template_folder="./templates/static")
app = Flask(__name__, template_folder="./templates/static")
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
# app.config["APPLICATION_ROOT"] = "/dev"


@app.route('/hello-world')
@cross_origin()
def hello_world():
    return jsonify('ServerlessMR Web Application')


@app.route("/url")
@cross_origin()
def url():
    return "The URL for this page is {}".format(url_for("/"))


# @app.route('/public/bundle.js')
# def custom_static():
#     return send_from_directory('templates/public', 'bundle.js')


@app.route('/public/<path:filename>')
def custom_static(filename):
    return send_from_directory('templates/public', filename)


@app.route('/')
@app.route('/index')
@app.route('/dev/table')
@cross_origin()
def index():
    return render_template("index.html")


@app.route('/username')
@cross_origin()
def get_username():
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
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
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
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
                submission_time = datetime.strptime(cur_job_info["submissionTime"], "%Y-%m-%d %H:%M:%S.%f")
                duration = datetime.utcnow() - submission_time
                cur_job_info['duration'] = str(duration)
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
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
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
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
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
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
        # local_endpoint_url = 'http://localhost:4586'
        # client = boto3.client('events',
        #                       region_name=StaticVariables.DEFAULT_REGION,
        #                       endpoint_url=local_endpoint_url)
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
    in_degree_obj = in_degree.InDegree(in_lambda=False)
    in_degrees = in_degree_obj.read_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME % job_name)
    return jsonify(in_degrees)


@app.route("/stage-progress", methods=['GET'])
@cross_origin()
def get_stage_progress():
    job_name = request.args.get('job-name')
    stage_progress_obj = stage_progress.StageProgress(in_lambda=False)
    stages_progress = stage_progress_obj.read_progress_table(StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % job_name)
    return jsonify(stages_progress)


@app.route("/num-completed-operators", methods=['GET'])
@cross_origin()
def get_num_completed_operators():
    job_name = request.args.get('job-name')
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False)
    s3_stage_conf_path = StaticVariables.S3_UI_STAGE_CONFIGURATION_PATH % job_name
    stage_config = json.loads(input_handler_s3_obj.read_records_from_input_key(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                                                               s3_stage_conf_path, static_job_info))
    stage_state_obj = stage_state.StageState(in_lambda=False)
    stage_states = stage_state_obj.read_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME % job_name)
    result = {}
    for stage_id, stage_num_completed in stage_states.items():
        result[stage_id] = [stage_num_completed, stage_config[stage_id]["num_operators"]]
    return jsonify(result)


@app.route("/dag", methods=['GET'])
@cross_origin()
def get_dag_information():
    job_name = request.args.get('job-name')
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False)
    s3_dag_information_path = StaticVariables.S3_UI_DAG_INFORMATION_PATH % job_name
    dag_data = json.loads(input_handler_s3_obj.read_records_from_input_key(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                                                           s3_dag_information_path, static_job_info))
    return jsonify(dag_data)


if __name__ == '__main__':
    app.run()