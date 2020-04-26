import json
import importlib.resources
import os
import shutil
import inspect

from pathlib import Path
from serverless_mr.driver.driver import Driver
from serverless_mr.functions.map_function import MapFunction
from serverless_mr.functions.map_shuffle_function import MapShuffleFunction
from serverless_mr.functions.reduce_function import ReduceFunction
from serverless_mr.driver.serverless_driver_setup import ServerlessDriverSetup
from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils.pipeline import Pipeline
from serverless_mr.default.partition import default_partition


project_working_dir = os.getcwd()
library_dir = Path(os.path.dirname(os.path.realpath(__file__)))
library_working_dir = library_dir.parent


def find_filepath(package_name, filename):
    with importlib.resources.path(package_name, filename) as path:
        return str(path)


def delete_dir(dirname):
    dst_dir = "%s/%s" % (library_dir, dirname)
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)


def delete_files(dirname, filenames):
    for filename in filenames:
        dst_file = "%s/%s/%s" % (library_dir, dirname, filename)
        if os.path.exists(dst_file):
            os.remove(dst_file)


def copy_files(dirname, dst_dirname, filenames):
    dst_dir = "%s/%s" % (library_dir, dst_dirname)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    for filename in filenames:
        path = find_filepath(dirname, filename)
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
    inspect_object = inspect.getfile(function)
    rel_filepath = os.path.relpath(inspect_object)
    print("The path of the function is", rel_filepath)
    # dst_file = "%s/%s/%s" % (library_working_dir, "user_job_3", "map.py")

    dst_file = "%s/%s" % (library_working_dir, rel_filepath)
    if os.path.normpath(inspect_object) != os.path.normpath(dst_file):
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        shutil.copy2(rel_filepath, dst_file)

    return rel_filepath


def tear_down():
    config_dirname = "configuration"
    delete_dir(config_dirname)
    delete_files("job", ["map.py", "reduce.py", "partition.py"])


class ServerlessMR:

    def __init__(self):
        self.pipelines = {}
        self.cur_pipeline = Pipeline()
        self.pipeline_id = 1
        self.total_num_functions = 0
        self.cur_last_map_index = -1
        self.last_partition_function = None
        self.last_combine_function = None

    def config(self, pipeline_specific_config):
        self.cur_pipeline.set_config(pipeline_specific_config)
        return self

    def map(self, map_function):
        rel_function_path = copy_job_function(map_function)
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
            self.last_partition_function = None

        map_function_obj = self.cur_pipeline.get_function_at_index(self.cur_last_map_index)
        map_function = map_function_obj.get_function()
        rel_map_function_path = map_function_obj.get_rel_function_path()
        rel_combiner_function_path = copy_job_function(combiner_function)
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

    # def merge_map_shuffle(self, map_function, partition_function, dependent_pipeline_ids):
    #     rel_map_function_path = copy_job_function(map_function)
    #     rel_partition_function_path = copy_job_function(partition_function)
    #     self.cur_pipeline.add_function(MergeMapShuffleFunction(map_function, rel_map_function_path,
    #                                                            partition_function, rel_partition_function_path))
    #     self.total_num_functions += 1
    #     return self

    def run(self):
        self.finish()
        set_up()
        StaticVariables.PROJECT_WORKING_DIRECTORY = project_working_dir
        StaticVariables.LIBRARY_WORKING_DIRECTORY = library_working_dir
        static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
        static_job_info = json.loads(static_job_info_file.read())
        static_job_info_file.close()

        is_serverless_driver = static_job_info[StaticVariables.SERVERLESS_DRIVER_FLAG_FN]

        if is_serverless_driver:
            pass
            serverless_driver_setup = ServerlessDriverSetup(self.pipelines, self.total_num_functions)
            serverless_driver_setup.register_driver()
            print("Driver Lambda function successfully registered")
            command = input("Enter invoke to invoke and other keys to exit: ")
            if command == "invoke":
                print("Driver invoked and starting job execution")
                serverless_driver_setup.invoke()
        else:
            print("The total number of functions is", self.total_num_functions)
            driver = Driver(self.pipelines, self.total_num_functions)
            driver.run()

        tear_down()
