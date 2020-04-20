import json


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


@app.route("/in-degree", methods=['GET'])
@cross_origin()
def get_in_degree_info():
    in_degree_obj = in_degree.InDegree(in_lambda=False)
    in_degrees = in_degree_obj.read_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME)
    return jsonify(in_degrees)


@app.route("/num-completed-operators", methods=['GET'])
@cross_origin()
def get_num_completed_operators():
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False)
    stage_config = json.loads(input_handler_s3_obj
                              .read_records_from_input_key(shuffling_bucket, StaticVariables.STAGE_CONFIGURATION_PATH, static_job_info))
    stage_state_obj = stage_state.StageState(in_lambda=False)
    stage_states = stage_state_obj.read_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)
    result = {}
    for stage_id, stage_num_completed in stage_states.items():
        result[stage_id] = [stage_num_completed, stage_config[stage_id]["num_operators"]]
    return jsonify(result)


@app.route("/dag", methods=['GET'])
@cross_origin()
def get_dag_information():
    static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
    shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
    input_handler_s3_obj = input_handler_s3.InputHandlerS3(in_lambda=False)
    pipelines_dependencies = json.loads(input_handler_s3_obj
                                        .read_records_from_input_key(shuffling_bucket, StaticVariables.PIPELINE_DEPENDENCIES_PATH, static_job_info))
    stage_mapping = json.loads(input_handler_s3_obj
                               .read_records_from_input_key(shuffling_bucket, StaticVariables.STAGE_TO_PIPELINE_PATH, static_job_info))
    stage_type_of_operations = json.loads(input_handler_s3_obj
                                          .read_records_from_input_key(shuffling_bucket, StaticVariables.STAGE_TYPE_OF_OPERATIONS, static_job_info))
    pipeline_first_last_stages = json.loads(input_handler_s3_obj
                                            .read_records_from_input_key(shuffling_bucket, StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH, static_job_info))

    print(pipelines_dependencies)
    print(stage_mapping)
    print(stage_type_of_operations)
    print(pipeline_first_last_stages)
    dag_data = {}
    nodes = []
    for i in range(1, len(stage_mapping) + 1):
        current_node = {'id': i, 'pipeline': -stage_mapping[str(i)],
                        'type': stage_type_of_operations[str(i)]}
        nodes.append(current_node)
    for i in range(1, len(pipeline_first_last_stages) + 1):
        current_node = {'id': -i}
        nodes.append(current_node)

    edges = []
    for i in range(1, len(stage_type_of_operations)):
        if stage_mapping[str(i)] == stage_mapping[str(i + 1)]:
            current_edge = {'source': i, 'target': i + 1}
            edges.append(current_edge)
        else:
            current_pipeline_id = stage_mapping[str(i)]
            for dependent_pipeline_id in pipelines_dependencies[str(current_pipeline_id)]:
                first_stage_id = pipeline_first_last_stages[str(dependent_pipeline_id)][0]
                current_edge = {'source': i, 'target': first_stage_id}
                edges.append(current_edge)

    dag_data['nodes'] = nodes
    dag_data['edges'] = edges
    return jsonify(dag_data)


if __name__ == '__main__':
    app.run()
