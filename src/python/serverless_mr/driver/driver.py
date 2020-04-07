import boto3
import json
import random
import time
import os
import pickle

from serverless_mr.utils import lambda_utils, zip, input_handler, output_handler, stage_state
from serverless_mr.aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config
from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.functions.map_shuffle_function import MapShuffleFunction
from serverless_mr.functions.reduce_function import ReduceFunction


def delete_files(dirname, filenames):
    for filename in filenames:
        dst_file = "%s/%s" % (dirname, filename)
        if os.path.exists(dst_file):
            os.remove(dst_file)


def delete_file(filename):
    if os.path.exists(filename):
        os.remove(filename)


class Driver:

    def __init__(self, functions, is_serverless=False):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        self.is_serverless = is_serverless
        self._set_aws_clients()
        self._set_lambda_config_and_client()
        self.cur_input_handler = input_handler.get_input_handler(self.static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                                 self.is_serverless)
        self.cur_output_handler = output_handler.get_output_handler(self.static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                                    self.is_serverless)
        self.functions = functions
        self.map_phase_state = stage_state.StageState(self.is_serverless)
        self._initialise_stage_state(len(self.functions))
        self.set_up_shuffling_bucket()
        self.set_up_output_bucket()

    def _set_aws_clients(self):
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
                                                region_name=StaticVariables.DEFAULT_REGION,
                                                endpoint_url=dynamodb_endpoint_url)
        else:
            self.s3_client = boto3.client('s3')
            self.dynamodb_client = boto3.client('dynamodb')

    def _set_lambda_config_and_client(self):
        region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
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

    def _initialise_stage_state(self, num_stages):
        self.map_phase_state.create_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)
        self.map_phase_state.initialise_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME,
                                                    num_stages)

    # Get all keys to be processed
    def _get_all_keys(self):
        lambda_memory = self.config[StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN] \
            if StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN in self.config else StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT
        concurrent_lambdas = self.config[StaticVariables.NUM_CONCURRENT_LAMBDAS_FN] \
            if StaticVariables.NUM_CONCURRENT_LAMBDAS_FN in self.config else StaticVariables.DEFAULT_NUM_CONCURRENT_LAMBDAS

        # Fetch all the keys that match the prefix
        all_keys = self.cur_input_handler.get_all_input_keys()

        print("The number of keys: ", len(all_keys))
        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
        print("The batch size is: ", bsize)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        print("The number of batches is the number of mappers: ", len(batches))
        num_mappers = len(batches)

        return all_keys, num_mappers, batches

    def _create_coordinator_config_file(self, cur_function, num_src_operators, num_dst_operators,
                                        invoking_lambda_name, function_pickle_path, partition_function_pickle_path="",
                                        reduce_function_pickle_path=""):
        config = {}
        config["num_src_operators"] = num_src_operators
        config["num_dst_operators"] = num_dst_operators
        config["invoking_lambda_name"] = invoking_lambda_name
        if isinstance(cur_function, ReduceFunction):
            config["coordinator_type"] = 2
        else:
            config["coordinator_type"] = 1
        config["function_pickle_path"] = function_pickle_path
        config["reduce_function_pickle_path"] = reduce_function_pickle_path
        config["partition_function_pickle_path"] = partition_function_pickle_path

        return config

    # Create the aws_lambda functions
    def _create_lambdas(self, num_src_operators):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        stage_id = 1
        function_lambdas = []
        coordinator_config = []

        # The first function should be a map/map_shuffle function
        for i in range(len(self.functions)):
            cur_function = self.functions[i]
            cur_function_zip_path = "serverless_mr/%s-%s.zip" % (cur_function.get_string(), stage_id)

            # Prepare Lambda functions if driver running in local machine
            if not self.is_serverless:
                cur_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % (cur_function.get_string(), stage_id)
                rel_function_paths = cur_function.get_rel_function_paths()
                with open(cur_function_pickle_path, 'wb') as f:
                    pickle.dump(cur_function.get_function(), f)
                if isinstance(cur_function, MapShuffleFunction):
                    partition_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % ("partition", stage_id)
                    with open(partition_function_pickle_path, 'wb') as f:
                        pickle.dump(cur_function.get_partition_function(), f)

                    next_reduce_function = self.functions[i+1]
                    reduce_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % \
                                                  (next_reduce_function.get_string(), stage_id + 1)
                    with open(reduce_function_pickle_path, 'wb') as f:
                        pickle.dump(next_reduce_function.get_function(), f)
                    rel_function_paths += next_reduce_function.get_rel_function_paths()

                zip.zip_lambda(rel_function_paths, cur_function_zip_path)

            cur_function_lambda_name = "%s-%s-%s-%s" % (job_name, lambda_name_prefix, cur_function.get_string(),
                                                        stage_id)
            cur_function_lambda = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                               cur_function_zip_path, job_name,
                                                               cur_function_lambda_name,
                                                               cur_function.get_handler_function_path())
            if isinstance(cur_function, MapShuffleFunction):
                assert i + 1 < len(self.functions) and isinstance(self.functions[i+1], ReduceFunction)
                cur_function_lambda.update_code_or_create_on_no_exist(len(self.functions), stage_id=stage_id,
                                                                      num_reducers=self.functions[i+1].get_num_reducers())
            else:
                cur_function_lambda.update_code_or_create_on_no_exist(len(self.functions), stage_id=stage_id)
            function_lambdas.append(cur_function_lambda)
            # delete_file(cur_function_zip_path)

            # Coordinator
            if i > 0 and not self.is_serverless:
                cur_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % (cur_function.get_string(), stage_id)
                if isinstance(cur_function, ReduceFunction):
                    num_reducers = cur_function.get_num_reducers()
                    coordinator_config.append(
                        self._create_coordinator_config_file(cur_function, num_src_operators,
                                                             num_reducers, cur_function_lambda_name,
                                                             cur_function_pickle_path)
                    )
                    num_src_operators = num_reducers
                elif isinstance(cur_function, MapShuffleFunction):
                    next_reduce_function = self.functions[i + 1]
                    partition_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % ("partition", stage_id)
                    reduce_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % \
                                                  (next_reduce_function.get_string(), stage_id + 1)
                    coordinator_config.append(
                        self._create_coordinator_config_file(cur_function, num_src_operators,
                                                             num_src_operators, cur_function_lambda_name,
                                                             cur_function_pickle_path, partition_function_pickle_path,
                                                             reduce_function_pickle_path)
                    )
                else:
                    coordinator_config.append(
                        self._create_coordinator_config_file(cur_function, num_src_operators,
                                                             num_src_operators, cur_function_lambda_name,
                                                             cur_function_pickle_path)
                    )

            stage_id += 1

        reduce_coordinator_zip_path = "serverless_mr/reduce-coordinator.zip"
        if not self.is_serverless:
            with open(StaticVariables.COORDINATOR_CONFIGURATION_PATH, 'w') as outfile:
                json.dump(coordinator_config, outfile)

            zip.zip_lambda([StaticVariables.REDUCE_COORDINATOR_HANDLER_PATH], reduce_coordinator_zip_path)

            # delete_file(StaticVariables.COORDINATOR_CONFIGURATION_PATH)

        cur_coordinator_lambda_name = "%s-%s-%s-%s" % (job_name, lambda_name_prefix, "reduce-coordinator",
                                                       stage_id)
        cur_coordinator_lambda = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                              reduce_coordinator_zip_path, job_name,
                                                              cur_coordinator_lambda_name,
                                                              StaticVariables.REDUCE_COORDINATOR_HANDLER_FUNCTION_PATH)
        cur_coordinator_lambda.update_code_or_create_on_no_exist(len(self.functions))
        cur_coordinator_lambda.add_lambda_permission(random.randint(1, 1000), shuffling_bucket)
        shuffling_s3_path_prefix = "%s/" % job_name
        cur_coordinator_lambda.create_s3_event_source_notification(shuffling_bucket, shuffling_s3_path_prefix)
        function_lambdas.append(cur_coordinator_lambda)

        if not self.is_serverless:
            # delete_file(reduce_coordinator_zip_path)
            pass

        return function_lambdas, num_src_operators

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

    def invoke_lambda(self, mapper_outputs, batches, mapper_id):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        first_function = self.functions[0]
        stage_id = 1
        first_function_lambda_name = "%s-%s-%s-%s" % (job_name, lambda_name_prefix, first_function.get_string(),
                                                      stage_id)

        pickle_file_path = 'serverless_mr/job/%s-%s.pkl' % (first_function.get_string(), stage_id)
        batch = [k['Key'] for k in batches[mapper_id - 1]]
        if isinstance(first_function, MapShuffleFunction):
            reduce_pickle_file_path = 'serverless_mr/job/%s-%s.pkl' % (self.functions[1].get_string(), stage_id+1)
            partition_pickle_file_path = 'serverless_mr/job/%s-%s.pkl' % ("partition", stage_id)
            resp = self.lambda_client.invoke(
                FunctionName=first_function_lambda_name,
                InvocationType='RequestResponse',
                Payload=json.dumps({
                    "keys": batch,
                    "id": mapper_id,
                    "function_pickle_path": pickle_file_path,
                    "reduce_function_pickle_path": reduce_pickle_file_path,
                    "partition_function_pickle_path": partition_pickle_file_path
                })
            )
        else:
            resp = self.lambda_client.invoke(
                FunctionName=first_function_lambda_name,
                InvocationType='RequestResponse',
                Payload=json.dumps({
                    "keys": batch,
                    "id": mapper_id,
                    "function_pickle_path": pickle_file_path
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

    def _calculate_cost(self, mapper_outputs, num_final_dst_operators):
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
                                                                                                            string_index,
                                                                                                            num_final_dst_operators)
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
        function_lambdas, num_final_dst_operators = self._create_lambdas(num_mappers)
        self._write_job_data(all_keys, num_mappers)

        # Execute
        # 3. Invoke Mappers and wait until they finish the execution
        mapper_outputs = self._invoke_mappers(num_mappers, batches)

        # 4. Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        self._calculate_cost(mapper_outputs, num_final_dst_operators)

        # 5. Delete the function lambdas
        for function_lambda in function_lambdas:
            function_lambda.delete_function()

        # 7. View one of the reducer results
        print(self.cur_output_handler.get_output(3))
        self.map_phase_state.delete_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)

        if not self.is_serverless:
            delete_files("serverless_mr/job", ["map.pkl", "reduce.pkl", "partition.pkl"])

    def set_up_shuffling_bucket(self):
        print("Setting up shuffling bucket")
        shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        self.s3_client.create_bucket(Bucket=shuffling_bucket)
        self.s3_client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=shuffling_bucket,
        )
        print("Finished setting up shuffling bucket")

    def set_up_output_bucket(self):
        print("Setting up output bucket")
        output_bucket = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]
        self.s3_client.create_bucket(Bucket=output_bucket)
        self.s3_client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=output_bucket,
        )
        print("Finished setting up shuffling bucket")

