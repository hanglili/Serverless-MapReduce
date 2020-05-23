import boto3
import json
import resource
import time
import os
import pickle
import random
import logging

from static.static_variables import StaticVariables
from utils import output_handler, stage_progress

static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.setLevel(level=logging.INFO)

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.reduce-handler')


def lambda_handler(event, _):
    logger.info("**************Reduce****************")
    start_time = time.time()
    io_time = 0

    reduce_keys = event['keys']
    reducer_id = event['id']
    reduce_function_pickle_path = event['function_pickle_path']

    with open(reduce_function_pickle_path, 'rb') as f:
        reduce_function = pickle.load(f)

    # create an S3 & Dynamo session
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
    # use_combine = static_job_info[StaticVariables.USE_COMBINE_FLAG_FN]
    job_name = static_job_info[StaticVariables.JOB_NAME_FN]

    stage_id = int(os.environ.get("stage_id"))
    total_num_stages = int(os.environ.get("total_num_stages"))
    coordinator_lambda_name = os.environ.get("coordinator_lambda_name")
    submission_time = os.environ.get("submission_time")

    logger.info("Stage: %s" % str(stage_id))
    logger.info("Reducer id: %s" % str(reducer_id))

    if StaticVariables.OPTIMISATION_FN not in static_job_info \
            or not static_job_info[StaticVariables.OPTIMISATION_FN]:
        stage_progress_obj = stage_progress.StageProgress(in_lambda=True,
                                                          is_local_testing=static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % (job_name, submission_time)

    # aggr
    line_count = 0
    intermediate_data = []
    retry_reduce_keys = []

    # INPUT JSON => OUTPUT JSON

    # Download and process all keys
    for key in reduce_keys:
        try:
            io_start_time = time.time()
            response = s3_client.get_object(Bucket=shuffling_bucket, Key=key)
            contents = response['Body'].read()
            io_time += time.time() - io_start_time

            for key_value in json.loads(contents):
                line_count += 1
                intermediate_data.append(key_value)
        except Exception as e:
            logger.info("Key: %s" % key)
            logger.info("First time Error: %s" % str(e))
            retry_reduce_keys.append(key)

    # time.sleep(1)
    # second_retry_reduce_keys = []
    # for key in retry_reduce_keys:
    #     try:
    #         io_start_time = time.time()
    #         response = s3_client.get_object(Bucket=shuffling_bucket, Key=key)
    #         contents = response['Body'].read()
    #         io_time += time.time() - io_start_time
    #
    #         for key_value in json.loads(contents):
    #             line_count += 1
    #             intermediate_data.append(key_value)
    #     except Exception as e:
    #         logger.info("Key: %s" % key)
    #         logger.info("Second time Error: %s" % str(e))
    #         second_retry_reduce_keys.append(key)

    # time.sleep(2)
    # for key in second_retry_reduce_keys:
    #     try:
    #         io_start_time = time.time()
    #         response = s3_client.get_object(Bucket=shuffling_bucket, Key=key)
    #         contents = response['Body'].read()
    #         io_time += time.time() - io_start_time
    #
    #         for key_value in json.loads(contents):
    #             line_count += 1
    #             intermediate_data.append(key_value)
    #     except Exception as e:
    #         logger.info("Key: %s" % key)
    #         logger.info("Third time Error: %s" % str(e))
    #         raise RuntimeError("%s" % str(e))

    intermediate_data.sort(key=lambda x: x[0])

    begin_time = time.time()
    interval_time = random.randint(60, 180)
    interval_num_keys_processed = 0
    average_num_keys = float(len(intermediate_data) / len(reduce_keys))

    cur_key = None
    cur_values = []
    outputs = []
    for key, value in intermediate_data:
        if cur_key == key:
            cur_values.append(value)
        else:
            if cur_key is not None:
                reduce_function(outputs, (cur_key, cur_values))

            cur_key = key
            cur_values = [value]

        if StaticVariables.OPTIMISATION_FN not in static_job_info \
                or not static_job_info[StaticVariables.OPTIMISATION_FN]:
            interval_num_keys_processed += 1
            current_time = time.time()
            if int(current_time - begin_time) > interval_time:
                begin_time = current_time
                interval_time = random.randint(1, 3)
                interval_num_files_processed = int(interval_num_keys_processed / average_num_keys)
                stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                               stage_id, interval_num_files_processed)
                interval_num_keys_processed = interval_num_keys_processed % average_num_keys

    if cur_key is not None:
        reduce_function(outputs, (cur_key, cur_values))

    if StaticVariables.OPTIMISATION_FN not in static_job_info \
            or not static_job_info[StaticVariables.OPTIMISATION_FN]:
        interval_num_files_processed = int(interval_num_keys_processed / average_num_keys)
        stage_progress_obj.increase_num_processed_keys(stage_progress_table_name,
                                                       stage_id, interval_num_files_processed)

    logger.info("Reduce sample outputs: %s" % str(outputs[0:10]))

    if stage_id == total_num_stages:
        cur_output_handler = output_handler.get_output_handler(static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                               static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                               in_lambda=True)
        io_start_time = time.time()
        cur_output_handler.write_output(reducer_id, outputs, {}, static_job_info, submission_time)
        io_time += time.time() - io_start_time
        logger.info("Finished writing the output")
    else:
        mapper_filename = "%s/%s-%s/%s" % (job_name, StaticVariables.OUTPUT_PREFIX, stage_id, reducer_id)
        io_start_time = time.time()
        s3_client.put_object(Bucket=shuffling_bucket, Key=mapper_filename,
                             Body=json.dumps(outputs))
        io_time += time.time() - io_start_time

        logger.info("Finished writing the output")

        lambda_client.invoke(
            FunctionName=coordinator_lambda_name,
            InvocationType='Event',
            Payload=json.dumps({
                'stage_id': stage_id
            })
        )

        logger.info("Finished scheduling the coordinator Lambda function")

    time_in_secs = time.time() - start_time
    metadata = {
        "lineCount": '%s' % line_count,
        "processingTime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "numKeys": '%s' % len(reduce_keys),
        "ioTime": '%s' % io_time,
        "computeTime": '%s' % str(time_in_secs - io_time)
    }

    info_write_start_time = time.time()
    metrics_bucket = StaticVariables.METRICS_BUCKET % job_name
    execution_info_s3_key = "%s/stage-%s/%s" % (job_name, stage_id, reducer_id)
    s3_client.put_object(Bucket=metrics_bucket, Key=execution_info_s3_key,
                         Body=json.dumps({}), Metadata=metadata)
    logger.info("Info write time: %s" % str(time.time() - info_write_start_time))

    logger.info("Reducer %s finishes execution" % str(reducer_id))
    logger.info("Execution time: %s" % str(time.time() - start_time))
