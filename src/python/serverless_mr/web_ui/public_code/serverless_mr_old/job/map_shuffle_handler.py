import boto3
import json
import resource
import time
import os
import pickle
import random


from static.static_variables import StaticVariables
from utils import input_handler, stage_progress


def lambda_handler(event, _):
    print("**************Map-Shuffle****************")
    start_time = time.time()

    src_keys = event['keys']
    mapper_id = event['id']
    load_data_from_input = event['load_data_from_input']
    map_function_pickle_path = event['function_pickle_path']
    combiner_function_pickle_path = event['combiner_function_pickle_path']
    partition_function_pickle_path = event['partition_function_pickle_path']

    with open(map_function_pickle_path, 'rb') as f:
        map_function = pickle.load(f)

    with open(combiner_function_pickle_path, 'rb') as f:
        combiner_function = pickle.load(f)

    with open(partition_function_pickle_path, 'rb') as f:
        partition_function = pickle.load(f)

    # create an S3 session
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

    shuffling_bucket = static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
    job_name = static_job_info[StaticVariables.JOB_NAME_FN]
    use_combine = static_job_info[StaticVariables.USE_COMBINE_FLAG_FN]

    stage_id = int(os.environ.get("stage_id"))
    num_bins = int(os.environ.get("num_reducers"))
    coordinator_lambda_name = os.environ.get("coordinator_lambda_name")
    submission_time = os.environ.get("submission_time")

    print("Stage:", stage_id)

    stage_progress_obj = stage_progress.StageProgress(in_lambda=True,
                                                      is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
    stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % (job_name, submission_time)

    # aggr
    line_count = 0
    err = ''

    # INPUT CSV => OUTPUT JSON

    begin_time = time.time()
    interval_time = random.randint(1, 3)
    interval_num_keys_processed = 0

    intermediate_data = []
    if load_data_from_input:
        cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                            static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                            in_lambda=True)
        input_source = static_job_info[StaticVariables.INPUT_SOURCE_FN]
        for input_key in src_keys:
            input_value = cur_input_handler.read_value(input_source, input_key, static_job_info)
            input_pair = (input_key, input_value)
            map_function(intermediate_data, input_pair)

            # TODO: Line count can be used to verify correctness of the job. Can be removed if needed in the future.
            if static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN] == "s3":
                line_count += len(input_value.split('\n')) - 1
            elif static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN] == "dynamodb":
                line_count += 1

            interval_num_keys_processed += 1
            current_time = time.time()
            if int(current_time - begin_time) > interval_time:
                begin_time = current_time
                interval_time = random.randint(1, 3)
                stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                               stage_id, interval_num_keys_processed)
                interval_num_keys_processed = 0
    else:
        for input_key in src_keys:
            response = s3_client.get_object(Bucket=shuffling_bucket, Key=input_key)
            contents = response['Body'].read()
            input_value = json.loads(contents)
            input_pair = (input_key, input_value)
            map_function(intermediate_data, input_pair)

            line_count += len(input_value)

            interval_num_keys_processed += 1
            current_time = time.time()
            if int(current_time - begin_time) > interval_time:
                begin_time = current_time
                interval_time = random.randint(1, 3)
                stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                               stage_id, interval_num_keys_processed)
                interval_num_keys_processed = 0

    stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                   stage_id, interval_num_keys_processed)

    if use_combine:

        intermediate_data.sort(key=lambda x: x[0])

        cur_key = None
        cur_values = []
        outputs = []
        for input_key, value in intermediate_data:
            if cur_key == input_key:
                cur_values.append(value)
            else:
                if cur_key is not None:
                    cur_key_outputs = []
                    combiner_function(cur_key_outputs, (cur_key, cur_values))
                    outputs += cur_key_outputs

                cur_key = input_key
                cur_values = [value]

        if cur_key is not None:
            cur_key_outputs = []
            combiner_function(cur_key_outputs, (cur_key, cur_values))
            outputs += cur_key_outputs

    else:
        outputs = intermediate_data

    time_in_secs = (time.time() - start_time)
    # timeTaken = time_in_secs * 1000000000 # in 10^9
    # s3DownloadTime = 0
    # totalProcessingTime = 0

    metadata = {
        "lineCount": '%s' % line_count,
        "processingTime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "numKeys": '%s' % len(src_keys)
    }

    # Partition ids are from 1 to n (inclusive).
    output_partitions = [[] for _ in range(num_bins + 1)]

    print("MapShuffle: outputs are", outputs[0:10])

    for input_key, value in outputs:
        partition_id = partition_function(input_key, num_bins) + 1
        cur_partition = output_partitions[partition_id]
        cur_partition.append(tuple((input_key, value)))

    for i in range(1, num_bins + 1):
        partition_id = "bin-%s" % i
        mapper_filename = "%s/%s-%s/%s/%s" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, partition_id, mapper_id)
        s3_client.put_object(Bucket=shuffling_bucket, Key=mapper_filename,
                             Body=json.dumps(output_partitions[i]), Metadata=metadata)

    lambda_client.invoke(
        FunctionName=coordinator_lambda_name,
        InvocationType='Event',
        Payload=json.dumps({
            'stage_id': stage_id
        })
    )
