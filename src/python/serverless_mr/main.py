import json
import importlib.resources
import os
import shutil
import inspect
import sys
import time
import logging

from pathlib import Path

project_working_dir = os.getcwd()
library_dir = Path(os.path.dirname(os.path.realpath(__file__)))
library_working_dir = library_dir
sys.path.insert(0, str(library_working_dir))

from driver.driver import Driver
from functions.map_function import MapFunction
from functions.map_shuffle_function import MapShuffleFunction
from functions.reduce_function import ReduceFunction
from driver.serverless_driver_setup import ServerlessDriverSetup
from static.static_variables import StaticVariables
from utils.pipeline import Pipeline
from default.partition import default_partition
from utils.setup_logger import logger

logger = logging.getLogger('serverless-mr.main')


def find_filepath(package_name, filename):
    try:
        with importlib.resources.path(package_name, filename) as path:
            return str(path)
    except Exception as e:
        logger.info("Configuration file %s not found" % filename)


def delete_dir(dirname):
    dst_dir = "%s/%s" % (library_dir, dirname)
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)


def delete_file(filename):
    dst_file = os.path.join(library_dir, filename)
    if os.path.exists(dst_file):
        os.remove(dst_file)


def copy_files(dirname, dst_dirname, filenames):
    dst_dir = "%s/%s" % (library_dir, dst_dirname)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    for filename in filenames:
        path = find_filepath(dirname, filename)
        if path is not None:
            if os.path.normpath(path) != os.path.normpath(dst_dir + "/" + filename):
                shutil.copy2(path, dst_dir)


def set_up():
    os.chdir(library_working_dir)
    config_dirname = "configuration"
    copy_files(config_dirname, config_dirname, ["static-job-info.json", "driver.json"])
    # copy_files("user_job", "job", ["map.py", "reduce.py", "partition.py"])

    # filepath = os.path.relpath(inspect.getfile(map_function))
    # print("The path of the map function is", filepath)
    # dst_file = "%s/%s/%s" % (library_working_dir, "user_job_3", "map.py")
    # shutil.copy2(filepath, dst_file)

    # outputs = []
    # map_function_(outputs, [1, '127.0.0.1, dasda, dasda, 1.0, dasdsa'])
    # print(outputs)

def copy_job_function(function):
    logger.info("Library working directory is %s" % library_working_dir)
    inspect_object = inspect.getfile(function)
    rel_filepath = os.path.relpath(inspect_object)
    logger.info("The path of the function is %s" % rel_filepath)
    # dst_file = "%s/%s/%s" % (library_working_dir, "user_job_3", "map.py")

    dst_file = "%s/%s" % (library_working_dir, rel_filepath)
    if os.path.normpath(inspect_object) != os.path.normpath(dst_file):
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        shutil.copy2(rel_filepath, dst_file)

    return rel_filepath


def tear_down(rel_function_paths):
    config_dirname = "configuration"
    delete_dir(config_dirname)

    for rel_function_path in rel_function_paths:
        dirname = os.path.dirname(rel_function_path)
        if dirname != '':
            delete_dir(dirname)
        else:
            delete_file(rel_function_path)


class ServerlessMR:

    def __init__(self):
        self.pipelines = {}
        self.cur_pipeline = Pipeline()
        self.pipeline_id = 1
        self.total_num_functions = 0
        self.cur_last_map_index = -1
        self.last_partition_function = None
        self.last_combine_function = None
        self.rel_function_paths = []

    def config(self, pipeline_specific_config):
        self.cur_pipeline.set_config(pipeline_specific_config)
        return self

    def map(self, map_function):
        rel_function_path = copy_job_function(map_function)
        self.rel_function_paths.append(rel_function_path)
        self.cur_pipeline.add_function(MapFunction(map_function, rel_function_path))
        self.total_num_functions += 1
        self.cur_last_map_index = self.cur_pipeline.get_num_functions() - 1
        return self

    def shuffle(self, partition_function):
        self.last_partition_function = partition_function
        return self

    def combine(self, combine_function):
        self.last_combine_function = combine_function
        return self

    def _construct_map_shuffle(self, combiner_function):
        if self.last_partition_function is None:
            partition_function = default_partition
            rel_partition_function_path = StaticVariables.DEFAULT_PARTITION_FUNCTION_PATH
        else:
            partition_function = self.last_partition_function
            rel_partition_function_path = copy_job_function(partition_function)
            self.rel_function_paths.append(rel_partition_function_path)
            self.last_partition_function = None

        map_function_obj = self.cur_pipeline.get_function_at_index(self.cur_last_map_index)
        map_function = map_function_obj.get_function()
        rel_map_function_path = map_function_obj.get_rel_function_path()
        rel_combiner_function_path = copy_job_function(combiner_function)
        self.rel_function_paths.append(rel_combiner_function_path)
        map_shuffle = MapShuffleFunction(map_function, rel_map_function_path, partition_function,
                                         rel_partition_function_path, combiner_function, rel_combiner_function_path)
        self.cur_pipeline.set_function_at_index(self.cur_last_map_index, map_shuffle)

    def reduce(self, reduce_function, num_reducers):
        if self.last_combine_function is None:
            self._construct_map_shuffle(reduce_function)
        else:
            self._construct_map_shuffle(self.last_combine_function)
            self.last_combine_function = None

        rel_function_path = copy_job_function(reduce_function)
        self.rel_function_paths.append(rel_function_path)
        self.cur_pipeline.add_function(ReduceFunction(reduce_function, rel_function_path, num_reducers))
        self.total_num_functions += 1
        return self

    def finish(self):
        cur_pipeline_id = self.pipeline_id
        self.pipelines[cur_pipeline_id] = self.cur_pipeline
        self.cur_pipeline = Pipeline()
        self.pipeline_id += 1
        self.cur_last_map_index = -1
        self.last_partition_function = None
        self.last_combine_function = None
        return cur_pipeline_id

    def merge(self, dependent_pipeline_ids):
        self.cur_pipeline.set_dependent_pipelines_ids(dependent_pipeline_ids)
        return self

    def run(self):
        StaticVariables.SETUP_START_TIME = time.time()
        self.finish()
        set_up()
        StaticVariables.PROJECT_WORKING_DIRECTORY = project_working_dir
        StaticVariables.LIBRARY_WORKING_DIRECTORY = library_working_dir
        static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
        static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()

        is_serverless_driver = static_job_info[StaticVariables.SERVERLESS_DRIVER_FLAG_FN]
        submission_time = ""

        if is_serverless_driver:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            os.chdir(StaticVariables.PROJECT_WORKING_DIRECTORY)
            main_file_path = os.path.relpath(module.__file__)
            os.chdir(StaticVariables.LIBRARY_WORKING_DIRECTORY)
            serverless_driver_setup = ServerlessDriverSetup(self.pipelines, self.total_num_functions)
            serverless_driver_setup.register_driver(main_file_path, self.rel_function_paths)
            logger.info("Driver Lambda function successfully registered")
            print("")
            command = input("Enter invoke to start the job and other keys to exit: ")
            if command == "invoke":
                logger.info("Driver invoked and starting job execution")
                serverless_driver_setup.invoke()
        else:
            logger.info("The total number of functions is %s" % self.total_num_functions)
            driver = Driver(self.pipelines, self.total_num_functions)
            submission_time = driver.run()

        tear_down(self.rel_function_paths)
        return submission_time
