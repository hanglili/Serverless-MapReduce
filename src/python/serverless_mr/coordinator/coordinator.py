import boto3
import json
import os

from static.static_variables import StaticVariables
from utils import stage_state, in_degree, stage_progress

# create an S3 and Lambda session
static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
    lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                 region_name=StaticVariables.DEFAULT_REGION,
                                 endpoint_url='http://%s:4574' % os.environ['LOCALSTACK_HOSTNAME'])
else:
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')


def get_map_reduce_outputs(bucket, job_name, stage_ids):
    keys_bins = []
    for stage_id in stage_ids:
        prefix = "%s/%s-%s/" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id)
        for obj in s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]:
            if not obj["Key"].endswith('/'):
                keys_bins.append([obj["Key"]])

    return keys_bins


def get_map_shuffle_outputs(num_bins, bucket, job_name, stage_id):
    keys_bins = [[] for _ in range(num_bins)]

    for bin_id in range(1, num_bins + 1):
        prefix = "%s/%s-%s/bin-%s/" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, bin_id)
        objs = s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]
        keys_bins[bin_id - 1] = [obj["Key"] for obj in objs]

    return keys_bins

def schedule_same_pipeline_next_stage(stage_configuration, stage_id, shuffling_bucket, job_name):
    cur_stage_config = stage_configuration[str(stage_id)]
    next_stage_config = stage_configuration[str(stage_id + 1)]
    invoking_lambda_name = next_stage_config["invoking_lambda_name"]
    next_stage_num_operators = stage_configuration[str(stage_id + 1)]["num_operators"]

    if cur_stage_config["stage_type"] == 1:
        keys_bins = get_map_shuffle_outputs(next_stage_num_operators, shuffling_bucket, job_name, stage_id)
    else:
        keys_bins = get_map_reduce_outputs(shuffling_bucket, job_name, [stage_id])

    stage_progress_obj = stage_progress.StageProgress(in_lambda=True,
                                                      is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
    stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % job_name
    total_num_jobs = sum([len(keys_bin) for keys_bin in keys_bins])
    stage_progress_obj.update_total_num_keys(stage_progress_table_name, stage_id + 1, total_num_jobs)

    if next_stage_config["stage_type"] == 1:
        for i in range(len(keys_bins)):
            response = lambda_client.invoke(
                FunctionName=invoking_lambda_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "keys": keys_bins[i],
                    "id": i + 1,
                    "load_data_from_input": False,
                    "function_pickle_path": next_stage_config["function_pickle_path"],
                    "combiner_function_pickle_path": next_stage_config["combiner_function_pickle_path"],
                    "partition_function_pickle_path": next_stage_config["partition_function_pickle_path"]
                })
            )

    else:
        for i in range(len(keys_bins)):
            response = lambda_client.invoke(
                FunctionName=invoking_lambda_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "keys": keys_bins[i],
                    "id": i + 1,
                    "load_data_from_input": False,
                    "function_pickle_path": next_stage_config["function_pickle_path"]
                })
            )

    print("All operators finished in stage %s, next stage: number of operators scheduled: %s"
          % (stage_id, next_stage_num_operators))


def schedule_different_pipeline_next_stage(is_serverless_driver, stage_configuration, cur_pipeline_id,
                                           shuffling_bucket, job_name):
    if not is_serverless_driver:
        with open(StaticVariables.PIPELINE_DEPENDENCIES_PATH) as json_file:
            adj_list = json.load(json_file)
    else:
        response = s3_client.get_object(Bucket=shuffling_bucket, Key=StaticVariables.PIPELINE_DEPENDENCIES_PATH)
        contents = response['Body'].read()
        adj_list = json.loads(contents)

    if not is_serverless_driver:
        with open(StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH) as json_file:
            pipeline_first_last_stage_ids = json.load(json_file)
    else:
        response = s3_client.get_object(Bucket=shuffling_bucket, Key=StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH)
        contents = response['Body'].read()
        pipeline_first_last_stage_ids = json.loads(contents)

    in_degree_obj = in_degree.InDegree(in_lambda=True,
                                       is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
    stage_progress_obj = stage_progress.StageProgress(in_lambda=True,
                                                      is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
    stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % job_name
    for dependent_pipeline_id in adj_list[str(cur_pipeline_id)]:
        response = in_degree_obj.decrement_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME % job_name,
                                                           dependent_pipeline_id)
        dependent_in_degree = int(response["Attributes"]["in_degree"]["N"])
        if dependent_in_degree == 0:
            next_pipeline_first_stage_id = pipeline_first_last_stage_ids[str(dependent_pipeline_id)][0]
            next_stage_config = stage_configuration[str(next_pipeline_first_stage_id)]
            invoking_lambda_name = next_stage_config["invoking_lambda_name"]
            dependent_stage_ids = next_stage_config["dependent_last_stage_ids"]
            # The last stages of a pipeline is assumed to be always either a map or reduce.
            keys_bins = get_map_reduce_outputs(shuffling_bucket, job_name, dependent_stage_ids)

            total_num_jobs = sum([len(keys_bin) for keys_bin in keys_bins])
            stage_progress_obj.update_total_num_keys(stage_progress_table_name, next_pipeline_first_stage_id, total_num_jobs)

            if next_stage_config["stage_type"] == 1:
                for i in range(len(keys_bins)):
                    response = lambda_client.invoke(
                        FunctionName=invoking_lambda_name,
                        InvocationType='Event',
                        Payload=json.dumps({
                            "keys": keys_bins[i],
                            "id": i + 1,
                            "load_data_from_input": False,
                            "function_pickle_path": next_stage_config["function_pickle_path"],
                            "combiner_function_pickle_path": next_stage_config["combiner_function_pickle_path"],
                            "partition_function_pickle_path": next_stage_config["partition_function_pickle_path"]
                        })
                    )

            else:
                for i in range(len(keys_bins)):
                    response = lambda_client.invoke(
                        FunctionName=invoking_lambda_name,
                        InvocationType='Event',
                        Payload=json.dumps({
                            "keys": keys_bins[i],
                            "id": i + 1,
                            "load_data_from_input": False,
                            "function_pickle_path": next_stage_config["function_pickle_path"]
                        })
                    )

            print("All operators finished in pipeline %s, next pipeline: number of operators scheduled: %s"
                  % (cur_pipeline_id, len(keys_bins)))


def lambda_handler(event, _):
    print("*************Coordinator****************")
    # start_time = time.time()

    # Shuffling Bucket (we just got a notification from this bucket)
    shuffling_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_obj_key = event['Records'][0]['s3']['object']['key']
    job_name = static_job_info[StaticVariables.JOB_NAME_FN]
    stage_s3_prefix = "%s/%s-" % (job_name, StaticVariables.OUTPUT_PREFIX)
    if not s3_obj_key.startswith(stage_s3_prefix):
        return

    cur_map_phase_state = stage_state.StageState(in_lambda=True,
                                                 is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
    # stage_id = cur_map_phase_state.read_current_stage_id(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)
    stage_id = int(s3_obj_key.split("/")[1].split("-")[1])
    print("Stage:", stage_id)
    is_serverless_driver = static_job_info[StaticVariables.SERVERLESS_DRIVER_FLAG_FN]
    if not is_serverless_driver:
        with open(StaticVariables.STAGE_CONFIGURATION_PATH) as json_file:
            stage_configuration = json.load(json_file)
    else:
        response = s3_client.get_object(Bucket=shuffling_bucket, Key=StaticVariables.STAGE_CONFIGURATION_PATH)
        contents = response['Body'].read()
        stage_configuration = json.loads(contents)

    cur_stage_config = stage_configuration[str(stage_id)]
    if cur_stage_config["stage_type"] == 1:
        next_stage_num_operators = stage_configuration[str(stage_id + 1)]["num_operators"]
        bin_s3_path = "bin-%s" % next_stage_num_operators
        shuffle_stage_s3_prefix = "%s/%s-%s/%s/" % (job_name, StaticVariables.OUTPUT_PREFIX,
                                                    stage_id, bin_s3_path)
        print("The current shuffle stage s3 prefix is", shuffle_stage_s3_prefix)
        print("The obj obj key is", s3_obj_key)
        if not s3_obj_key.startswith(shuffle_stage_s3_prefix):
            return

    print("The event obj key is", s3_obj_key)
    num_operators = cur_stage_config["num_operators"]
    response = cur_map_phase_state.increment_num_completed_operators(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME % job_name,
                                                                     stage_id)
    num_completed_operators = int(response["Attributes"]["num_completed_operators"]["N"])
    print("In stage %s, number of operators completed: %s" % (stage_id, num_completed_operators))

    if num_operators == num_completed_operators:
        if not is_serverless_driver:
            with open(StaticVariables.STAGE_TO_PIPELINE_PATH) as f:
                mapping_stage_id_pipeline_id = json.load(f)
        else:
            response = s3_client.get_object(Bucket=shuffling_bucket, Key=StaticVariables.STAGE_TO_PIPELINE_PATH)
            contents = response['Body'].read()
            mapping_stage_id_pipeline_id = json.loads(contents)

        cur_pipeline_id = mapping_stage_id_pipeline_id[str(stage_id)]
        if cur_pipeline_id != mapping_stage_id_pipeline_id[str(stage_id + 1)]:
            schedule_different_pipeline_next_stage(is_serverless_driver, stage_configuration, cur_pipeline_id, shuffling_bucket, job_name)
        else:
            schedule_same_pipeline_next_stage(stage_configuration, stage_id, shuffling_bucket, job_name)

        # cur_map_phase_state.increment_current_stage_id(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)
    else:
        print("Waiting for all the operators of the stage %s to finish" % stage_id)

'''
ev = {
    "Records": [{'s3': {'bucket': {'name': "smallya-useast-1"}}}],
    "bucket": "smallya-useast-1",
    "jobId": "jobid134",
    "mapCount": 1,
    "reducerFunctionName": "shell-exec",
    "reducerHandler": "index.handler"
}
lambda_handler(ev, {})
'''