import json
import boto3
import pickle
import os

from serverless_mr.driver.driver import set_up_local_input_data
from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import zip
from serverless_mr.aws_lambda import lambda_manager
from botocore.client import Config
from serverless_mr.functions.map_shuffle_function import MapShuffleFunction
from serverless_mr.functions.merge_map_shuffle import MergeMapShuffleFunction


def delete_files(filenames):
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)


class ServerlessDriverSetup:
    def __init__(self, pipelines, total_num_functions):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                          region_name=StaticVariables.DEFAULT_REGION,
                                          endpoint_url='http://localhost:4572')
        else:
            self.s3_client = boto3.client('s3')

        self.region = self.config[StaticVariables.REGION_FN] \
            if StaticVariables.REGION_FN in self.config else StaticVariables.DEFAULT_REGION
        lambda_read_timeout = self.config[StaticVariables.LAMBDA_READ_TIMEOUT_FN] \
            if StaticVariables.LAMBDA_READ_TIMEOUT_FN in self.config else StaticVariables.DEFAULT_LAMBDA_READ_TIMEOUT
        boto_max_connections = self.config[StaticVariables.BOTO_MAX_CONNECTIONS_FN] \
            if StaticVariables.BOTO_MAX_CONNECTIONS_FN in self.config else StaticVariables.DEFAULT_BOTO_MAX_CONNECTIONS
        lambda_name_prefix = self.static_job_info[StaticVariables.LAMBDA_NAME_PREFIX_FN]
        self.job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        self.driver_lambda_name = lambda_name_prefix + "-driver-" + self.job_name

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        lambda_config = Config(read_timeout=lambda_read_timeout,
                               max_pool_connections=boto_max_connections,
                               region_name=self.region)
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            self.lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                         region_name=self.region,
                                         endpoint_url='http://localhost:4574', config=lambda_config)
        else:
            self.lambda_client = boto3.client('lambda', config=lambda_config)

        self.pipelines = pipelines
        self.total_num_functions = total_num_functions

    def _overwrite_existing_job_info(self, pipeline_specific_config):
        with open(StaticVariables.STATIC_JOB_INFO_PATH, "r") as f:
            cur_config = json.load(f)

        for key, value in pipeline_specific_config.items():
            cur_config[key] = value

        with open(StaticVariables.STATIC_JOB_INFO_PATH, "w") as f:
            json.dump(cur_config, f)

        return cur_config

    # Serverless set up
    def register_driver(self):
        stage_id = 1
        function_filepaths = []

        # The first function should be a map/map_shuffle function
        for pipeline_id, pipeline in self.pipelines.items():
            functions = pipeline.get_functions()
            pipeline_static_job_info = self._overwrite_existing_job_info(pipeline.get_config())
            # TODO: The next line is correct?
            self.static_job_info = pipeline_static_job_info
            dependent_pipeline_ids = pipeline.get_dependent_pipeline_ids()
            if len(dependent_pipeline_ids) == 0:
                set_up_local_input_data(pipeline_static_job_info)
            for i in range(len(functions)):
                cur_function = functions[i]
                cur_function_zip_path = "serverless_mr/%s-%s.zip" % (cur_function.get_string(), stage_id)

                # Prepare Lambda functions if driver running in local machine
                cur_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % (cur_function.get_string(), stage_id)
                rel_function_paths = cur_function.get_rel_function_paths()
                with open(cur_function_pickle_path, 'wb') as f:
                    pickle.dump(cur_function.get_function(), f)
                if isinstance(cur_function, MapShuffleFunction) \
                        or isinstance(cur_function, MergeMapShuffleFunction):
                    partition_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % ("partition", stage_id)
                    with open(partition_function_pickle_path, 'wb') as f:
                        pickle.dump(cur_function.get_partition_function(), f)

                    next_function_reduce = functions[i + 1]
                    reduce_function_pickle_path = 'serverless_mr/job/%s-%s.pkl' % \
                                                  (next_function_reduce.get_string(), stage_id + 1)
                    with open(reduce_function_pickle_path, 'wb') as f:
                        pickle.dump(next_function_reduce.get_function(), f)
                    rel_function_paths += next_function_reduce.get_rel_function_paths()

                zip.zip_lambda(rel_function_paths, cur_function_zip_path)

                function_filepaths += rel_function_paths
                stage_id += 1

        with open(StaticVariables.SERVERLESS_PIPELINES_INFO_PATH, 'wb') as f:
            pickle.dump(self.pipelines, f)
        with open(StaticVariables.SERVERLESS_TOTAL_NUM_OPERATIONS_PATH, 'wb') as f:
            pickle.dump(self.total_num_functions, f)

        zip.zip_lambda([StaticVariables.COORDINATOR_HANDLER_PATH], StaticVariables.COORDINATOR_ZIP_PATH)

        zip.zip_driver_lambda(self.config[StaticVariables.DRIVER_FN][StaticVariables.ZIP_FN], function_filepaths)

        l_driver = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, self.region,
                                                self.config[StaticVariables.DRIVER_FN][StaticVariables.ZIP_FN],
                                                self.job_name, self.driver_lambda_name,
                                                self.config[StaticVariables.DRIVER_FN][StaticVariables.HANDLER_FN])
        l_driver.update_code_or_create_on_no_exist(self.total_num_functions)

        # delete_files(glob.glob(StaticVariables.FUNCTIONS_PICKLE_GLOB_PATH))

    def invoke(self):
        result = self.lambda_client.invoke(
            FunctionName=self.driver_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )

        print("Finished executing this job: ", result)
        # delete_files([cur_function_zip_path])
