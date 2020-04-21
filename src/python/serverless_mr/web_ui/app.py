import json
import boto3


from datetime import datetime
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS, cross_origin
from serverless_mr.utils import in_degree, stage_state
from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.data_sources import input_handler_s3


app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
@cross_origin()
def hello_world():
    return jsonify('ServerlessMR Web Application')


@app.route('/index')
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
        if not obj["Key"].endswith('/') and "static-job-info.json" in obj["Key"]:
            job_keys_list.append(obj["Key"])

    jobs_information = {}
    completed = []
    active = []
    for job_key in job_keys_list:
        response = client.get_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                     Key=job_key)
        contents = response['Body'].read()
        cur_static_job_info = json.loads(contents)
        if cur_static_job_info["completed"]:
            completed.append(cur_static_job_info)
        else:
            submission_time = datetime.strptime(cur_static_job_info["submissionTime"], "%Y-%m-%d %H:%M:%S.%f")
            duration = datetime.now() - submission_time
            cur_static_job_info['duration'] = str(duration)
            active.append(cur_static_job_info)

    jobs_information["completed"] = completed
    jobs_information["active"] = active
    return jsonify(jobs_information)


@app.route("/in-degree", methods=['GET'])
@cross_origin()
def get_in_degree_info():
    job_name = request.args.get('job-name')
    in_degree_obj = in_degree.InDegree(in_lambda=False)
    in_degrees = in_degree_obj.read_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME % job_name)
    return jsonify(in_degrees)


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
