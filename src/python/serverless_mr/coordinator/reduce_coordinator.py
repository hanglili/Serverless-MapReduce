import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import stage_state

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


def get_src_operator_outputs(bucket, job_name, stage_id):
    keys = []
    prefix = "%s/%s-%s/" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id)
    for obj in s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]:
        if not obj["Key"].endswith('/'):
            keys.append(obj["Key"])

    return keys


def get_map_shuffle_outputs(num_bins, bucket, job_name, stage_id):
    bins_of_keys = [[] for _ in range(num_bins + 1)]

    for i in range(1, num_bins + 1):
        prefix = "%s/%s-%s/bin-%s/" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, i)
        keys = s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]
        bins_of_keys[i] = keys

    return bins_of_keys


def lambda_handler(event, _):
    print("**************Reduce coordinator****************")
    # start_time = time.time()

    # Shuffling Bucket (we just got a notification from this bucket)
    shuffling_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_obj_key = event['Records'][0]['s3']['object']['key']
    job_name = static_job_info[StaticVariables.JOB_NAME_FN]
    stage_s3_prefix = "%s/%s-" % (job_name, StaticVariables.OUTPUT_PREFIX)
    if not s3_obj_key.startswith(stage_s3_prefix):
        return

    cur_map_phase_state = stage_state.StageState(in_lambda=True)
    stage_id = cur_map_phase_state.read_current_stage_id(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)
    with open(StaticVariables.COORDINATOR_CONFIGURATION_PATH) as json_file:
        coordinator_config = json.load(json_file)
    cur_stage_config = coordinator_config[stage_id - 1]
    if cur_stage_config["coordinator_type"] == 2:
        num_dst_operators = cur_stage_config["num_dst_operators"]
        bin_s3_path = "bin-%s" % num_dst_operators
        shuffle_stage_s3_prefix = "%s/%s-%s/%s/" % (job_name, StaticVariables.OUTPUT_PREFIX,
                                                    stage_id, bin_s3_path)
        print("The current suffle stage s3 prefix is", shuffle_stage_s3_prefix)
        print("The obj obj key is", s3_obj_key)
        if not s3_obj_key.startswith(shuffle_stage_s3_prefix):
            return

    print("The event obj key is", s3_obj_key)
    num_src_operators = cur_stage_config["num_src_operators"]
    response = cur_map_phase_state.increment_num_completed_operators(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME,
                                                                     stage_id)
    num_src_completed_operators = int(response["Attributes"]["num_completed_operators"]["N"])
    print("Number of operators completed in stage %s is %s" % (stage_id, num_src_completed_operators))

    if num_src_operators == num_src_completed_operators:
        invoking_lambda_name = cur_stage_config["invoking_lambda_name"]
        if cur_stage_config["coordinator_type"] == 1:
            # Map -> Any\Reduce, Reduce -> Any\Reduce
            keys = get_src_operator_outputs(shuffling_bucket, job_name, stage_id)
            for i in range(len(keys)):
                response = lambda_client.invoke(
                    FunctionName=invoking_lambda_name,
                    InvocationType='Event',
                    Payload=json.dumps({
                        "keys": [keys[i]],
                        "id": i + 1,
                        "function_pickle_path": cur_stage_config["function_pickle_path"],
                        "reduce_function_pickle_path": cur_stage_config["reduce_function_pickle_path"],
                        "partition_function_pickle_path": cur_stage_config["partition_function_pickle_path"]
                    })
                )
            print("Finished scheduling %s number of operators at stage %s" % (len(keys), stage_id))
        elif cur_stage_config["coordinator_type"] == 2:
            # Map Shuffling -> Reduce
            num_dst_operators = cur_stage_config["num_dst_operators"]
            bins_of_keys = get_map_shuffle_outputs(num_dst_operators, shuffling_bucket, job_name, stage_id)
            for i in range(1, num_dst_operators + 1):
                cur_bin_of_keys = [b["Key"] for b in bins_of_keys[i]]
                response = lambda_client.invoke(
                    FunctionName=invoking_lambda_name,
                    InvocationType='Event',
                    Payload=json.dumps({
                        "keys": cur_bin_of_keys,
                        "id": i,
                        "function_pickle_path": cur_stage_config["function_pickle_path"]
                    })
                )
            print("Finished scheduling %s number of operators at stage %s" % (num_dst_operators, stage_id))
        else:
            print("Error: Unknown coordinator type")
            return

        cur_map_phase_state.increment_current_stage_id(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)

    else:
        print("Still waiting for all the src operators to finish...")


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