import boto3
import json
import random
import time
import os
import pickle
import glob

from datetime import datetime
from collections import defaultdict
from utils import lambda_utils, zip, input_handler, output_handler, stage_state, in_degree, stage_progress
from aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config
from static.static_variables import StaticVariables
from functions.map_shuffle_function import MapShuffleFunction
from functions.reduce_function import ReduceFunction


def delete_files(filenames):
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)


def set_up_local_input_data(static_job_info):
    if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
        cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                            static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])

        os.chdir(StaticVariables.PROJECT_WORKING_DIRECTORY)
        local_testing_input_path = static_job_info[StaticVariables.LOCAL_TESTING_INPUT_PATH]
        local_file_paths = glob.glob(local_testing_input_path + "*")
        print("The current working directory for local file paths is:", os.getcwd())
        print("The list of local file paths:", local_file_paths)
        cur_input_handler.set_up_local_input_data(local_file_paths, static_job_info)
        os.chdir(StaticVariables.LIBRARY_WORKING_DIRECTORY)


def create_stage_config_file(num_operators, stage_type, invoking_lambda_name,
                             function_pickle_path, dependent_last_stage_ids,
                             partition_function_pickle_path="", combiner_function_pickle_path=""):
    return {
        "num_operators": num_operators, "invoking_lambda_name": invoking_lambda_name,
        "function_pickle_path": function_pickle_path,
        "combiner_function_pickle_path": combiner_function_pickle_path,
        "partition_function_pickle_path": partition_function_pickle_path,
        "dependent_last_stage_ids": dependent_last_stage_ids,
        "stage_type": stage_type
    }


def pickle_functions_and_zip_stage(cur_function_zip_path, cur_function, stage_id):
    cur_function_pickle_path = 'job/%s-%s.pkl' % (cur_function.get_string(), stage_id)
    rel_function_paths = cur_function.get_rel_function_paths()
    with open(cur_function_pickle_path, 'wb') as f:
        pickle.dump(cur_function.get_function(), f)
    if isinstance(cur_function, MapShuffleFunction):
        partition_function_pickle_path = 'job/%s-%s.pkl' % ("partition", stage_id)
        with open(partition_function_pickle_path, 'wb') as f:
            pickle.dump(cur_function.get_partition_function(), f)

        combiner_function_pickle_path = 'job/%s-%s.pkl' % ("combiner", stage_id)
        with open(combiner_function_pickle_path, 'wb') as f:
            pickle.dump(cur_function.get_combiner_function(), f)

    zip.zip_lambda(rel_function_paths, cur_function_zip_path)
    return rel_function_paths


def construct_dag_information(pipelines_dependencies, stage_mapping, pipeline_first_last_stages,
                              stage_type_of_operations):
    dag_data = {}
    nodes = []
    for i in range(1, len(stage_mapping) + 1):
        current_node = {'id': i, 'pipeline': -stage_mapping[i],
                        'type': stage_type_of_operations[i]}
        nodes.append(current_node)
    for i in range(1, len(pipeline_first_last_stages) + 1):
        current_node = {'id': -i}
        nodes.append(current_node)

    edges = []
    for i in range(1, len(stage_type_of_operations)):
        if stage_mapping[i] == stage_mapping[i + 1]:
            current_edge = {'source': i, 'target': i + 1}
            edges.append(current_edge)
        else:
            current_pipeline_id = stage_mapping[i]
            for dependent_pipeline_id in pipelines_dependencies[current_pipeline_id]:
                first_stage_id = pipeline_first_last_stages[dependent_pipeline_id][0]
                current_edge = {'source': i, 'target': first_stage_id}
                edges.append(current_edge)

    dag_data['nodes'] = nodes
    dag_data['edges'] = edges
    return dag_data


def populate_static_job_info(static_job_info, total_num_pipelines, total_num_stages, submission_time):
    static_job_info["completed"] = False
    static_job_info["submissionTime"] = submission_time
    static_job_info["duration"] = -1
    static_job_info["totalNumPipelines"] = total_num_pipelines
    static_job_info["totalNumStages"] = total_num_stages


class Driver:

    def __init__(self, pipelines, total_num_functions, is_serverless=False):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        self.is_serverless = is_serverless
        self._set_aws_clients()
        self._set_lambda_config_and_client()
        self.pipelines = pipelines
        self.total_num_functions = total_num_functions
        self.submission_time = datetime.utcnow().strftime("%Y-%m-%d_%H.%M.%S")
        self.map_phase_state = stage_state.StageState(self.is_serverless,
                                                      is_local_testing=self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        self._initialise_stage_state(total_num_functions)
        prefix = "%s/" % self.static_job_info[StaticVariables.JOB_NAME_FN]
        self.set_up_bucket(self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN])
        self.delete_s3_objects(self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN], prefix)
        self.set_up_bucket(StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME)

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
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        table_name = StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME % (job_name, self.submission_time)
        self.map_phase_state.delete_state_table(table_name)
        self.map_phase_state.create_state_table(table_name)
        self.map_phase_state.initialise_state_table(table_name, num_stages)

    def delete_s3_objects(self, bucket_name, prefix):
        response = self.s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            contents = self.s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]
            delete_keys = []
            for content in contents:
                delete_keys.append({'Key': content["Key"]})

            self.s3_client.delete_objects(Bucket=bucket_name, Delete={
                'Objects': delete_keys
            })

    def set_up_bucket(self, bucket_name):
        self.s3_client.create_bucket(Bucket=bucket_name)
        s3_bucket_exists_waiter = self.s3_client.get_waiter('bucket_exists')
        s3_bucket_exists_waiter.wait(Bucket=bucket_name)
        self.s3_client.put_bucket_acl(
            ACL='public-read-write',
            Bucket=bucket_name,
        )
        print("%s Bucket created successfully" % bucket_name)

    # Get all keys to be processed
    def _get_all_keys(self, static_job_info):
        lambda_memory = self.config[StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN] \
            if StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN in self.config \
            else StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT
        concurrent_lambdas = self.config[StaticVariables.NUM_CONCURRENT_LAMBDAS_FN] \
            if StaticVariables.NUM_CONCURRENT_LAMBDAS_FN in self.config \
            else StaticVariables.DEFAULT_NUM_CONCURRENT_LAMBDAS

        # Fetch all the keys that match the prefix
        cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN],
                                                            static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                            self.is_serverless)
        all_keys = cur_input_handler.get_all_input_keys(static_job_info)

        print("The number of keys: ", len(all_keys))
        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas,
                                                static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN])
        print("The batch size is: ", bsize)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        print("The number of batches is the number of mappers: ", len(batches))
        num_mappers = len(batches)

        return all_keys, num_mappers, batches

    def _overwrite_existing_job_info(self, pipeline_specific_config):
        with open(StaticVariables.STATIC_JOB_INFO_PATH, "r") as f:
            cur_config = json.load(f)

        for key, value in pipeline_specific_config.items():
            cur_config[key] = value

        if not self.is_serverless:
            with open(StaticVariables.STATIC_JOB_INFO_PATH, "w") as f:
                json.dump(cur_config, f)

        return cur_config

    # Create the aws_lambda functions
    def _create_lambdas(self):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        stage_id = 1
        num_operators = 0
        function_lambdas = []
        stage_config = {}
        mapping_stage_id_pipeline_id = {}
        adj_list = defaultdict(list)
        in_degrees = {}
        invoking_pipelines_info = {}
        pipelines_last_stage_num_operators = {}
        pipelines_first_last_stage_ids = {}
        stage_type_of_operations = {}
        cur_coordinator_lambda_name = "%s-%s-%s" % (job_name, lambda_name_prefix, "coordinator")

        # The first function should be a map/map_shuffle function
        for pipeline_id, pipeline in self.pipelines.items():
            functions = pipeline.get_functions()
            pipeline_static_job_info = self._overwrite_existing_job_info(pipeline.get_config())
            # TODO: The next line is correct?
            self.static_job_info = pipeline_static_job_info
            dependent_pipeline_ids = pipeline.get_dependent_pipeline_ids()
            for dependent_pipeline_id in dependent_pipeline_ids:
                adj_list[dependent_pipeline_id].append(pipeline_id)
                in_degrees[pipeline_id] = in_degrees.get(pipeline_id, 0) + 1

            if len(dependent_pipeline_ids) == 0:
                if not self.is_serverless:
                    set_up_local_input_data(pipeline_static_job_info)
                all_keys, num_operators, batches = self._get_all_keys(pipeline_static_job_info)
                first_function = functions[0]
                invoking_pipelines_info[pipeline_id] = [all_keys, num_operators, batches, first_function, stage_id]
            else:
                num_operators = 0
                for dependent_pipeline_id in dependent_pipeline_ids:
                    num_operators += pipelines_last_stage_num_operators[dependent_pipeline_id]

            pipelines_first_last_stage_ids[pipeline_id] = [stage_id]

            for i in range(len(functions)):
                mapping_stage_id_pipeline_id[stage_id] = pipeline_id
                cur_function = functions[i]
                cur_function_zip_path = "%s-%s.zip" % (cur_function.get_string(), stage_id)
                stage_type_of_operations[stage_id] = cur_function.get_string()

                # Prepare Lambda functions if driver running in local machine
                if not self.is_serverless:
                    pickle_functions_and_zip_stage(cur_function_zip_path, cur_function, stage_id)

                cur_function_lambda_name = "%s-%s-%s-%s" % (job_name, lambda_name_prefix, cur_function.get_string(),
                                                            stage_id)
                cur_function_lambda = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                                   cur_function_zip_path, job_name,
                                                                   cur_function_lambda_name,
                                                                   cur_function.get_handler_function_path())
                if isinstance(cur_function, MapShuffleFunction):
                    assert i + 1 < len(functions) and isinstance(functions[i+1], ReduceFunction)
                    cur_function_lambda.update_code_or_create_on_no_exist(self.total_num_functions,
                                                                          submission_time=self.submission_time,
                                                                          coordinator_lambda_name=cur_coordinator_lambda_name,
                                                                          stage_id=stage_id,
                                                                          num_reducers=functions[i+1].get_num_reducers())
                else:
                    cur_function_lambda.update_code_or_create_on_no_exist(self.total_num_functions,
                                                                          submission_time=self.submission_time,
                                                                          coordinator_lambda_name=cur_coordinator_lambda_name,
                                                                          stage_id=stage_id)
                function_lambdas.append(cur_function_lambda)

                # Coordinator
                cur_function_pickle_path = 'job/%s-%s.pkl' % (cur_function.get_string(), stage_id)
                dependent_last_stage_ids = []
                for dependent_pipeline_id in dependent_pipeline_ids:
                    dependent_last_stage_ids.append(pipelines_first_last_stage_ids[dependent_pipeline_id][1])
                if isinstance(cur_function, MapShuffleFunction):
                    partition_function_pickle_path = 'job/%s-%s.pkl' % ("partition", stage_id)
                    combiner_function_pickle_path = 'job/%s-%s.pkl' % ("combiner", stage_id)
                    stage_config[stage_id] = \
                        create_stage_config_file(num_operators, 1, cur_function_lambda_name,
                                                 cur_function_pickle_path, dependent_last_stage_ids,
                                                 partition_function_pickle_path,
                                                 combiner_function_pickle_path)
                else:
                    if isinstance(cur_function, ReduceFunction):
                        num_operators = cur_function.get_num_reducers()

                    stage_config[stage_id] = \
                        create_stage_config_file(num_operators, 2, cur_function_lambda_name,
                                                 cur_function_pickle_path,
                                                 dependent_last_stage_ids)

                stage_id += 1

            pipelines_first_last_stage_ids[pipeline_id].append(stage_id - 1)
            pipelines_last_stage_num_operators[pipeline_id] = num_operators

        coordinator_zip_path = StaticVariables.COORDINATOR_ZIP_PATH
        if not self.is_serverless:
            self._write_config_to_local(adj_list, mapping_stage_id_pipeline_id, pipelines_first_last_stage_ids,
                                        stage_config)

            zip.zip_lambda([StaticVariables.COORDINATOR_HANDLER_PATH], coordinator_zip_path)
        else:
            self._write_config_to_s3(adj_list, mapping_stage_id_pipeline_id, pipelines_first_last_stage_ids,
                                     stage_config, shuffling_bucket)

        # Web UI information
        dag_information = construct_dag_information(adj_list, mapping_stage_id_pipeline_id,
                                                    pipelines_first_last_stage_ids, stage_type_of_operations)
        populate_static_job_info(self.static_job_info, len(pipelines_first_last_stage_ids),
                                 len(stage_type_of_operations), self.submission_time)
        self._write_web_ui_info(dag_information, stage_config, self.static_job_info,
                                StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME, job_name)

        cur_coordinator_lambda = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                              coordinator_zip_path, job_name,
                                                              cur_coordinator_lambda_name,
                                                              StaticVariables.COORDINATOR_HANDLER_FUNCTION_PATH)
        cur_coordinator_lambda.update_code_or_create_on_no_exist(self.total_num_functions,
                                                                 submission_time=self.submission_time)
        # cur_coordinator_lambda.add_lambda_permission(random.randint(1, 1000), shuffling_bucket)
        # shuffling_s3_path_prefix = "%s/" % job_name
        # cur_coordinator_lambda.create_s3_event_source_notification(shuffling_bucket, shuffling_s3_path_prefix)
        # time.sleep(1)
        function_lambdas.append(cur_coordinator_lambda)

        in_degree_obj = in_degree.InDegree(in_lambda=self.is_serverless,
                                           is_local_testing=self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        in_degree_table_name = StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME % (job_name, self.submission_time)
        in_degree_obj.delete_in_degree_table(in_degree_table_name)
        in_degree_obj.create_in_degree_table(in_degree_table_name)
        in_degree_obj.initialise_in_degree_table(in_degree_table_name, in_degrees)

        stage_progress_obj = stage_progress.StageProgress(in_lambda=self.is_serverless,
                                                          is_local_testing=self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % (job_name, self.submission_time)
        stage_progress_obj.delete_progress_table(stage_progress_table_name)
        stage_progress_obj.create_progress_table(stage_progress_table_name)
        stage_progress_obj.initialise_progress_table(stage_progress_table_name, stage_id - 1)

        if not self.is_serverless:
            delete_files(glob.glob(StaticVariables.LAMBDA_ZIP_GLOB_PATH))
            delete_files(glob.glob(StaticVariables.FUNCTIONS_PICKLE_GLOB_PATH))

        return function_lambdas, invoking_pipelines_info, num_operators

    def _write_web_ui_info(self, dag_information, stage_config, static_job_info, bucket_name, job_name):
        self.s3_client.put_object(Bucket=bucket_name,
                                  Key=(StaticVariables.S3_UI_STAGE_CONFIGURATION_PATH
                                       % (job_name, self.submission_time)),
                                  Body=json.dumps(stage_config))
        self.s3_client.put_object(Bucket=bucket_name,
                                  Key=(StaticVariables.S3_UI_DAG_INFORMATION_PATH
                                       % (job_name, self.submission_time)),
                                  Body=json.dumps(dag_information))
        self.s3_client.put_object(Bucket=bucket_name,
                                  Key=(StaticVariables.S3_UI_GENERAL_JOB_INFORMATION_PATH
                                       % (job_name, self.submission_time)),
                                  Body=json.dumps(static_job_info))

    def _write_config_to_s3(self, adj_list, mapping_stage_id_pipeline_id, pipelines_first_last_stage_ids,
                            stage_config, shuffling_bucket):
        self.s3_client.put_object(Bucket=shuffling_bucket, Key=StaticVariables.STAGE_CONFIGURATION_PATH,
                                  Body=json.dumps(stage_config))
        self.s3_client.put_object(Bucket=shuffling_bucket, Key=StaticVariables.PIPELINE_DEPENDENCIES_PATH,
                                  Body=json.dumps(adj_list))
        self.s3_client.put_object(Bucket=shuffling_bucket, Key=StaticVariables.STAGE_TO_PIPELINE_PATH,
                                  Body=json.dumps(mapping_stage_id_pipeline_id))
        self.s3_client.put_object(Bucket=shuffling_bucket, Key=StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH,
                                  Body=json.dumps(pipelines_first_last_stage_ids))

    def _write_config_to_local(self, adj_list, mapping_stage_id_pipeline_id, pipelines_first_last_stage_ids,
                               stage_config):
        with open(StaticVariables.STAGE_CONFIGURATION_PATH, 'w') as f:
            json.dump(stage_config, f)
        with open(StaticVariables.PIPELINE_DEPENDENCIES_PATH, 'w') as f:
            json.dump(adj_list, f)
        with open(StaticVariables.STAGE_TO_PIPELINE_PATH, 'w') as f:
            json.dump(mapping_stage_id_pipeline_id, f)
        with open(StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH, 'w') as f:
            json.dump(pipelines_first_last_stage_ids, f)

    def invoke_lambda(self, batches, first_function, stage_id, mapper_id):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        first_function_lambda_name = "%s-%s-%s-%s" % (job_name, lambda_name_prefix, first_function.get_string(),
                                                      stage_id)

        function_pickle_path = 'job/%s-%s.pkl' % (first_function.get_string(), stage_id)
        batch = [k['Key'] for k in batches[mapper_id - 1]]
        if isinstance(first_function, MapShuffleFunction):
            combiner_function_pickle_path = 'job/%s-%s.pkl' % ("combiner", stage_id)
            partition_function_pickle_path = 'job/%s-%s.pkl' % ("partition", stage_id)
            response = self.lambda_client.invoke(
                FunctionName=first_function_lambda_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "keys": batch,
                    "id": mapper_id,
                    "load_data_from_input": True,
                    "function_pickle_path": function_pickle_path,
                    "combiner_function_pickle_path": combiner_function_pickle_path,
                    "partition_function_pickle_path": partition_function_pickle_path
                })
            )
            print("The response is", response)
        else:
            response = self.lambda_client.invoke(
                FunctionName=first_function_lambda_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "keys": batch,
                    "id": mapper_id,
                    "load_data_from_input": True,
                    "function_pickle_path": function_pickle_path
                })
            )
            print("The response is", response)

    def _invoke_pipelines(self, invoking_pipelines_info):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        stage_progress_obj = stage_progress.StageProgress(in_lambda=self.is_serverless,
                                                          is_local_testing=self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN])
        stage_progress_table_name = StaticVariables.STAGE_PROGRESS_DYNAMODB_TABLE_NAME % (job_name, self.submission_time)
        for pipeline_id, invoking_pipeline_info in invoking_pipelines_info.items():
            print("Scheduling pipeline %s" % pipeline_id)
            num_mappers = invoking_pipeline_info[1]
            batches = invoking_pipeline_info[2]
            first_function = invoking_pipeline_info[3]
            stage_id = invoking_pipeline_info[4]
            concurrent_lambdas = self.config[StaticVariables.NUM_CONCURRENT_LAMBDAS_FN] \
                if StaticVariables.NUM_CONCURRENT_LAMBDAS_FN in self.config else StaticVariables.DEFAULT_NUM_CONCURRENT_LAMBDAS

            total_num_jobs = sum([len(batch) for batch in batches])
            stage_progress_obj.update_total_num_keys(stage_progress_table_name, stage_id, total_num_jobs)
            # Exec Parallel
            print("Number of Mappers: ", num_mappers)
            pool = ThreadPool(num_mappers)
            ids = [i + 1 for i in range(num_mappers)]
            invoke_lambda_partial = partial(self.invoke_lambda, batches, first_function, stage_id)

            # Burst request handling
            mappers_executed = 0
            while mappers_executed < num_mappers:
                nm = min(concurrent_lambdas, num_mappers)
                results = pool.map(invoke_lambda_partial, ids[mappers_executed: mappers_executed + nm])
                mappers_executed += nm

            pool.close()
            pool.join()

            print("Pipeline %s scheduled successfully" % pipeline_id)


    def _calculate_cost(self, num_outputs, cur_output_handler, invoking_pipelines_info):
        total_lambda_time = 0
        intermediate_s3_get_ops = 0
        intermediate_s3_put_ops = 0
        intermediate_s3_size = 0
        total_lines = 0

        pipelines_first_stage_ids = [pipeline_info[4] for _, pipeline_info in invoking_pipelines_info.items()]
        lambda_memory = self.config[StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN] \
            if StaticVariables.LAMBDA_MEMORY_PROVISIONED_FN in self.config \
            else StaticVariables.DEFAULT_LAMBDA_MEMORY_LIMIT
        shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]

        # Wait for the job to complete so that we can compute total cost ; create a poll every 5 secs
        while True:
            print("Checking whether the job is completed...")
            response, string_index = cur_output_handler.list_objects_for_checking_finish(self.static_job_info,
                                                                                         self.submission_time)
            if string_index in response:
                last_stage_lambda_time, last_stage_storage_cost, last_stage_write_cost, last_stage_read_cost = \
                    cur_output_handler.check_job_finish(response, string_index, num_outputs, self.submission_time,
                                                        self.static_job_info)
                if last_stage_lambda_time > -1:
                    total_lambda_time += last_stage_lambda_time
                    last_stage_database_cost = last_stage_storage_cost + last_stage_write_cost + last_stage_read_cost
                    break
            time.sleep(5)

        print("Job Complete")

        response = self.s3_client.list_objects(Bucket=shuffling_bucket, Prefix=job_name)
        if "Contents" in response:
            all_stages_key_objs = response["Contents"]
            for all_stage_key_obj in all_stages_key_objs:
                # Even though metadata processing time is written as processingTime,
                # AWS does not accept uppercase letter metadata key
                all_stage_key = all_stage_key_obj["Key"]
                intermediate_s3_size += all_stage_key_obj["Size"]
                intermediate_s3_get_ops += 1
                intermediate_s3_put_ops += 1
                if ("bin" not in all_stage_key) or ("bin-1" in all_stage_key):
                    lambda_time = float(self.s3_client.get_object(Bucket=shuffling_bucket,
                                                                  Key=all_stage_key)['Metadata']['processingtime'])
                    total_lambda_time += lambda_time
                    stage_id = int(all_stage_key.split("/")[1].split("-")[1])
                    if stage_id in pipelines_first_stage_ids:
                        total_lines += int(self.s3_client.get_object(Bucket=shuffling_bucket,
                                                                     Key=all_stage_key)['Metadata']['linecount'])

        # S3 Storage cost for shuffling bucket and output bucket - is negligible anyways since S3 costs 3 cents/GB/month
        # Storage cost per GB / hour
        intermediate_s3_storage_cost = 1 * 0.0000521574022522109 \
                                              * (intermediate_s3_size / 1024.0 / 1024.0 / 1024.0)
        # S3 PUT # 0.005/1000
        intermediate_s3_put_cost = intermediate_s3_put_ops * 0.005 / 1000
        # S3 GET # $0.004/10000
        intermediate_s3_get_cost = intermediate_s3_get_ops * 0.004 / 10000

        # Lambda cost - For 1024 MB Lambda, it costs $0.00001667/s
        total_lambda_cost = total_lambda_time * 0.00001667 * lambda_memory / 1024.0
        total_intermediate_s3_cost = (intermediate_s3_get_cost + intermediate_s3_put_cost +
                                      intermediate_s3_storage_cost)

        print("Intermediate Stages total number of s3 GET ops:", intermediate_s3_get_ops)
        print("Intermediate Stages total number of s3 PUT ops:", intermediate_s3_put_ops)
        print("********** COST ***********")
        print("Total Lambda Cost:", total_lambda_cost)
        print("Intermediate Stages S3 Storage Cost:", intermediate_s3_storage_cost)
        print("Intermediate Stages S3 Request Cost:", intermediate_s3_get_cost + intermediate_s3_put_cost)
        print("Total Intermediate Stages S3 Cost:", total_intermediate_s3_cost)
        print("Total Last Stage Database Cost:", last_stage_database_cost)
        print("Total Cost:", total_lambda_cost + total_intermediate_s3_cost + last_stage_database_cost)
        print("Total Lines:", total_lines)

    def _update_duration(self):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        s3_job_info_path = StaticVariables.S3_UI_GENERAL_JOB_INFORMATION_PATH % (job_name, self.submission_time)
        response = self.s3_client.get_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                             Key=s3_job_info_path)
        contents = response['Body'].read()
        cur_static_job_info = json.loads(contents)
        submission_time = datetime.strptime(cur_static_job_info["submissionTime"], "%Y-%m-%d_%H.%M.%S")
        duration = datetime.utcnow() - submission_time
        cur_static_job_info['duration'] = str(duration).split(".")[0]
        cur_static_job_info['completed'] = True

        self.s3_client.put_object(Bucket=StaticVariables.S3_JOBS_INFORMATION_BUCKET_NAME,
                                  Key=s3_job_info_path,
                                  Body=json.dumps(cur_static_job_info))

    def run(self):
        # 1. Create the aws_lambda functions
        function_lambdas, invoking_pipelines_info, num_outputs = self._create_lambdas()

        # Execute
        # 2. Invoke Mappers and wait until they finish the execution
        self._invoke_pipelines(invoking_pipelines_info)

        # 3. Create output storage and calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        cur_output_handler = output_handler.get_output_handler(self.static_job_info[StaticVariables.OUTPUT_SOURCE_TYPE_FN],
                                                               self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN],
                                                               self.is_serverless)
        cur_output_handler.create_output_storage(self.submission_time, self.static_job_info)
        self._calculate_cost(num_outputs, cur_output_handler, invoking_pipelines_info)

        # 4. Delete the function lambdas
        for function_lambda in function_lambdas:
            function_lambda.delete_function()

        # 5. View one of the reducer results
        print(cur_output_handler.get_output(3, self.static_job_info, self.submission_time))
        # self.map_phase_state.delete_state_table(StaticVariables.STAGE_STATE_DYNAMODB_TABLE_NAME)

        self._update_duration()
        
        # in_degree_obj = in_degree.InDegree(in_lambda=self.is_serverless)
        # in_degree_obj.delete_in_degree_table(StaticVariables.IN_DEGREE_DYNAMODB_TABLE_NAME)
