import boto3
import json
import resource
import time
import os
import pickle
import random
import logging


from static.static_variables import StaticVariables
from utils import input_handler, output_handler, stage_progress

static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.setLevel(level=logging.INFO)

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.map-handler')


def lambda_handler(event, _):
    logger.info("**************Map****************")
    start_time = time.time()
    io_time = 0

    src_keys = event['keys']
    load_data_from_input = event['load_data_from_input']
    mapper_id = event['id']
    map_function_pickle_path = event['function_pickle_path']

    with open(map_function_pickle_path, 'rb') as f:
        map_function = pickle.load(f)

    # create an S3 session
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

    stage_id = int(os.environ.get("stage_id"))
    total_num_stages = int(os.environ.get("total_num_stages"))
    coordinator_lambda_name = os.environ.get("coordinator_lambda_name")
    submission_time = os.environ.get("submission_time")

    logger.info("Stage: %s" % stage_id)

    if StaticVariables.OPTIMISATION_FN not in static_job_info \
            or not static_job_info[StaticVariables.OPTIMISATION_FN]:
        stage_progress_obj = stage_progress.StageProgress(in_lambda=True,
                                                          is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % (job_name, submission_time)
    # aggr
    line_count = 0

    # INPUT CSV => OUTPUT JSON

    begin_time = time.time()
    interval_time = random.randint(1, 3)
    interval_num_keys_processed = 0

    outputs = []

    start_overhead = time.time() - start_time
    logger.info("Start overhead: %s" % str(start_overhead))

    if load_data_from_input:
        cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                            static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                            in_lambda=True)
        input_source = static_job_info[StaticVariables.INPUT_SOURCE_FN]
        for input_key in src_keys:
            io_start_time = time.time()
            input_value = cur_input_handler.read_value(input_source, input_key, static_job_info)
            io_time += time.time() - io_start_time
            input_pair = (input_key, input_value)
            map_function(outputs, input_pair)

            # TODO: Line count can be used to verify correctness of the job. Can be removed if needed in the future.
            if StaticVariables.OPTIMISATION_FN not in static_job_info \
                    or not static_job_info[StaticVariables.OPTIMISATION_FN]:
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
            io_start_time = time.time()
            response = s3_client.get_object(Bucket=shuffling_bucket, Key=input_key)
            contents = response['Body'].read()
            input_value = json.loads(contents)
            io_time += time.time() - io_start_time
            input_pair = (input_key, input_value)
            map_function(outputs, input_pair)

            if StaticVariables.OPTIMISATION_FN not in static_job_info \
                    or not static_job_info[StaticVariables.OPTIMISATION_FN]:
                line_count += len(input_value)

                interval_num_keys_processed += 1
                current_time = time.time()
                if int(current_time - begin_time) > interval_time:
                    begin_time = current_time
                    interval_time = random.randint(1, 3)
                    stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                                   stage_id, interval_num_keys_processed)
                    interval_num_keys_processed = 0

    if StaticVariables.OPTIMISATION_FN not in static_job_info \
            or not static_job_info[StaticVariables.OPTIMISATION_FN]:
        stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                       stage_id, interval_num_keys_processed)

    # timeTaken = time_in_secs * 1000000000 # in 10^9
    # s3DownloadTime = 0
    # totalProcessingTime = 0

    logger.info("Map sample outputs: %s" % str(outputs[0:10]))

    if stage_id == total_num_stages:
        cur_output_handler = output_handler.get_output_handler(static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                               static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                               in_lambda=True)
        # cur_output_handler.write_output(mapper_id, outputs, metadata, submission_time, static_job_info)
        io_start_time = time.time()
        cur_output_handler.write_output(mapper_id, outputs, {}, static_job_info, submission_time)
        io_time += time.time() - io_start_time
    else:
        mapper_filename = "%s/%s-%s/%s" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, mapper_id)
        # s3_client.put_object(Bucket=shuffling_bucket, Key=mapper_filename,
        #                      Body=json.dumps(outputs), Metadata=metadata)
        io_start_time = time.time()
        s3_client.put_object(Bucket=shuffling_bucket, Key=mapper_filename, Body=json.dumps(outputs))
        io_time += time.time() - io_start_time

        lambda_client.invoke(
            FunctionName=coordinator_lambda_name,
            InvocationType='Event',
            Payload=json.dumps({
                'stage_id': stage_id
            })
        )

    execution_time = time.time() - start_time
    metadata = {
        "lineCount": '%s' % line_count,
        "processingTime": '%s' % execution_time,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "numKeys": '%s' % len(src_keys),
        "ioTime": '%s' % io_time,
        "computeTime": '%s' % str(execution_time - io_time)
    }

    info_write_start_time = time.time()
    metrics_bucket = StaticVariables.METRICS_BUCKET % job_name
    execution_info_s3_key = "%s/stage-%s/%s" % (job_name, stage_id, mapper_id)
    s3_client.put_object(Bucket=metrics_bucket, Key=execution_info_s3_key,
                         Body=json.dumps({}), Metadata=metadata)
    logger.info("Info write time: %s" % str(time.time() - info_write_start_time))

    logger.info("Mapper %s finishes execution" % str(mapper_id))
    logger.info("Execution time: %s" % str(time.time() - start_time))
