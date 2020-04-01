import boto3
import json
import random
import time
import os

from serverless_mr.utils import lambda_utils, zip, input_handler, output_handler, map_phase_state
from serverless_mr.aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config
from serverless_mr.static.static_variables import StaticVariables


class Driver:

    def __init__(self, is_serverless=False):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        self.is_serverless = is_serverless
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if not self.is_serverless:
                s3_endpoint_url = 'http://localhost:4572'
                dynamodb_endpoint_url = 'http://localhost:4569'
            else:
                s3_endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
                dynamodb_endpoint_url = 'http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME']
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                          region_name=StaticVariables.DEFAULT_REGION, endpoint_url=s3_endpoint_url)
            self.dynamodb_client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                                region_name=StaticVariables.DEFAULT_REGION, endpoint_url=dynamodb_endpoint_url)
        else:
            self.s3_client = boto3.client('s3')
            self.dynamodb_client = boto3.client('dynamodb')
        self.lambda_config = None
        self.lambda_client = None
        self.cur_input_handler = input_handler.get_input_handler(self.static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                                 self.is_serverless)
        self.cur_output_handler = output_handler.get_output_handler(self.static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                                    self.is_serverless)
        self.map_phase_state = map_phase_state.MapPhaseState(self.is_serverless)
        self._initialise_map_phase_state()

    def _initialise_map_phase_state(self):
        self.map_phase_state.create_state_table(StaticVariables.MAPPER_PHASE_STATE_DYNAMODB_TABLE_NAME)
        self.map_phase_state.initialise_state_table(StaticVariables.MAPPER_PHASE_STATE_DYNAMODB_TABLE_NAME)

    # Get all keys to be processed
    def _get_all_keys(self):
        region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        lambda_memory = self.config[StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN] \
            if StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN in self.config else StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT
        concurrent_lambdas = self.config[StaticVariables.NUM_CONCURRENT_LAMBDAS_FN] \
            if StaticVariables.NUM_CONCURRENT_LAMBDAS_FN in self.config else StaticVariables.DEFAULT_NUM_CONCURRENT_LAMBDAS
        lambda_read_timeout = self.config[StaticVariables.LAMBDA_READ_TIMEOUT_FN] \
            if StaticVariables.LAMBDA_READ_TIMEOUT_FN in self.config else StaticVariables.DEFAULT_LAMBDA_READ_TIMEOUT
        boto_max_connections = self.config[StaticVariables.BOTO_MAX_CONNECTIONS_FN] \
            if StaticVariables.BOTO_MAX_CONNECTIONS_FN in self.config else StaticVariables.DEFAULT_BOTO_MAX_CONNECTIONS

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        self.lambda_config = Config(read_timeout=lambda_read_timeout,
                                    max_pool_connections=boto_max_connections,
                                    region_name=region)

        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if not self.is_serverless:
                endpoint_url = 'http://localhost:4574'
            else:
                endpoint_url = 'http://%s:4574' % os.environ['LOCALSTACK_HOSTNAME']
            self.lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                              region_name=region,
                                              endpoint_url=endpoint_url, config=self.lambda_config)
        else:
            self.lambda_client = boto3.client('lambda', config=self.lambda_config)

        # Fetch all the keys that match the prefix
        all_keys = self.cur_input_handler.get_all_input_keys()

        print("The number of keys: ", len(all_keys))
        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
        print("The batch size is: ", bsize)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        print("The number of batches is the number of mappers: ", len(batches))
        num_mappers = len(batches)

        return all_keys, num_mappers, batches

    # Create the aws_lambda functions
    def _create_lambda(self, num_mappers):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]

        mapper_lambda_name = lambda_name_prefix + "-mapper-" + job_name
        reducer_lambda_name = lambda_name_prefix + "-reducer-" + job_name
        rc_lambda_name = lambda_name_prefix + "-reduce-coordinator-" + job_name

        shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        num_reducers = self.static_job_info[StaticVariables.NUM_REDUCER_FN]

        # Prepare Lambda functions if driver running in local machine
        if not self.is_serverless:
            zip.zip_lambda(self.config[StaticVariables.MAPPER_FN][StaticVariables.LOCATION_FN],
                           self.config[StaticVariables.MAPPER_FN][StaticVariables.ZIP_FN])
            zip.zip_lambda(self.config[StaticVariables.REDUCER_FN][StaticVariables.LOCATION_FN],
                           self.config[StaticVariables.REDUCER_FN][StaticVariables.ZIP_FN])
            zip.zip_lambda(self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.LOCATION_FN],
                           self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.ZIP_FN])

        # Mapper
        l_mapper = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                self.config[StaticVariables.MAPPER_FN][StaticVariables.ZIP_FN],
                                                job_name, mapper_lambda_name,
                                                self.config[StaticVariables.MAPPER_FN][StaticVariables.HANDLER_FN])
        l_mapper.update_code_or_create_on_no_exist()

        # Reducer
        l_reducer = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                 self.config[StaticVariables.REDUCER_FN][StaticVariables.ZIP_FN], job_name,
                                                 reducer_lambda_name,
                                                 self.config[StaticVariables.REDUCER_FN][StaticVariables.HANDLER_FN])
        l_reducer.update_code_or_create_on_no_exist()

        # Coordinator
        l_rc = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                            self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.ZIP_FN],
                                            job_name, rc_lambda_name,
                                            self.config[StaticVariables.REDUCER_COORDINATOR_FN][StaticVariables.HANDLER_FN])
        l_rc.update_code_or_create_on_no_exist(str(num_mappers))

        # Add permission to the coordinator
        l_rc.add_lambda_permission(random.randint(1, 1000), shuffling_bucket)

        # create event source for coordinator
        last_bin_path = "%s/%s/bin%s/" % (job_name, StaticVariables.MAP_OUTPUT_PREFIX, str(num_reducers))
        l_rc.create_s3_event_source_notification(shuffling_bucket, last_bin_path)
        return l_mapper, l_reducer, l_rc

    # Write Jobdata to S3
    def _write_job_data(self, all_keys, n_mappers):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        j_key = "%s/%s" % (job_name, StaticVariables.JOB_DATA_S3_FILENAME)
        data = json.dumps({
            "mapCount": n_mappers,
            "totalS3Files": len(all_keys),
            "startTime": time.time()
        })

        self.s3_client.put_object(Bucket=self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN], Key=j_key, Body=data, Metadata={})

    def invoke_lambda(self, mapper_outputs, batches, m_id):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        mapper_lambda_name = lambda_name_prefix + "-mapper-" + job_name

        batch = [k['Key'] for k in batches[m_id - 1]]
        resp = self.lambda_client.invoke(
            FunctionName=mapper_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "keys": batch,
                "mapperId": m_id
            })
        )
        out = eval(resp['Payload'].read())
        mapper_outputs.append(out)
        print("Mapper processing information: ", out)

    def _invoke_mappers(self, num_mappers, batches):
        mapper_outputs = []
        concurrent_lambdas = self.config[StaticVariables.NUM_CONCURRENT_LAMBDAS_FN] \
            if StaticVariables.NUM_CONCURRENT_LAMBDAS_FN in self.config else StaticVariables.DEFAULT_NUM_CONCURRENT_LAMBDAS

        # Exec Parallel
        print("Number of Mappers: ", num_mappers)
        pool = ThreadPool(num_mappers)
        ids = [i + 1 for i in range(num_mappers)]
        invoke_lambda_partial = partial(self.invoke_lambda, mapper_outputs, batches)

        # Burst request handling
        mappers_executed = 0
        while mappers_executed < num_mappers:
            nm = min(concurrent_lambdas, num_mappers)
            results = pool.map(invoke_lambda_partial, ids[mappers_executed: mappers_executed + nm])
            mappers_executed += nm

        pool.close()
        pool.join()

        print("All the mappers have finished")
        return mapper_outputs

    def _calculate_cost(self, mapper_outputs):
        total_lambda_secs = 0
        total_s3_get_ops = 0
        # total_s3_put_ops = 0
        # s3_storage_hours = 0
        total_lines = 0

        for output in mapper_outputs:
            total_s3_get_ops += int(output[0])
            total_lines += int(output[1])
            total_lambda_secs += float(output[2])

        # Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs
        # Get all reducer keys

        # Total execution time for reducers
        lambda_memory = self.config[StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN] \
            if StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN in self.config else StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT

        while True:
            print("Checking whether the job is completed...")
            response, string_index = self.cur_output_handler.list_objects_for_checking_finish()
            if string_index in response:
                reducer_lambda_time, total_s3_size, len_job_keys = self.cur_output_handler.check_job_finish(response,
                                                                                                            string_index)
                if reducer_lambda_time > -1:
                    break
            time.sleep(5)

        print("Job Complete")

        # S3 Storage cost - Account for mappers only; This cost is negligible anyways since S3 costs 3 cents/GB/month
        s3_storage_hour_cost = 1 * 0.0000521574022522109 * (total_s3_size / 1024.0 / 1024.0 / 1024.0)  # cost per GB/hr
        s3_put_cost = len_job_keys * 0.005 / 1000

        # S3 GET # $0.004/10000
        total_s3_get_ops += len_job_keys
        s3_get_cost = total_s3_get_ops * 0.004 / 10000

        total_lambda_secs += reducer_lambda_time
        lambda_cost = total_lambda_secs * 0.00001667 * lambda_memory / 1024.0
        s3_cost = (s3_get_cost + s3_put_cost + s3_storage_hour_cost)

        print("Reducer L", reducer_lambda_time * 0.00001667 * lambda_memory / 1024.0)
        print("Lambda Cost", lambda_cost)
        print("S3 Storage Cost", s3_storage_hour_cost)
        print("S3 Request Cost", s3_get_cost + s3_put_cost)
        print("S3 Cost", s3_cost)
        print("Total Cost: ", lambda_cost + s3_cost)
        print("Total Lines:", total_lines)

    def run(self):
        # 1. Get all keys to be processed
        all_keys, num_mappers, batches = self._get_all_keys()

        # 2. Create the aws_lambda functions
        l_mapper, l_reducer, l_rc = self._create_lambda(num_mappers)
        self._write_job_data(all_keys, num_mappers)

        # Execute
        # 3. Invoke Mappers and wait until they finish the execution
        mapper_outputs = self._invoke_mappers(num_mappers, batches)

        # 4. Delete Mapper function
        l_mapper.delete_function()

        # 5. Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        self._calculate_cost(mapper_outputs)

        # 6. Delete Reducer and its coordinator function
        l_reducer.delete_function()
        l_rc.delete_function()

        # 7. View one of the reducer results
        print(self.cur_output_handler.get_output(3))
