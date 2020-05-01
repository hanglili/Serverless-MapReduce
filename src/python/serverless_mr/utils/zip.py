import glob
import subprocess
import os
from zipfile import ZipFile

from static.static_variables import StaticVariables


def zip_lambda(filenames, zip_name):
    globed_results = []
    for filename in filenames:
        globed_results += glob.glob(filename)
    print("The globed results are", globed_results)
    file_paths = globed_results + glob.glob(StaticVariables.STATIC_JOB_INFO_PATH)\
                 + glob.glob(StaticVariables.LAMBDA_UTILS_PATH) + glob.glob(StaticVariables.MAP_HANDLER_PATH)\
                 + glob.glob(StaticVariables.REDUCE_HANDLER_PATH) + glob.glob(StaticVariables.JOB_INIT_PATH)\
                 + glob.glob(StaticVariables.UTILS_INIT_PATH) + glob.glob(StaticVariables.PARTITION_PATH)\
                 + glob.glob(StaticVariables.COMBINE_PATH) + glob.glob(StaticVariables.STATIC_INIT_PATH)\
                 + glob.glob(StaticVariables.STATIC_VARIABLES_PATH) + glob.glob(StaticVariables.REDUCE_PATH)\
                 + glob.glob(StaticVariables.INPUT_HANDLER_PATH) + glob.glob(StaticVariables.OUTPUT_HANDLER_PATH)\
                 + glob.glob(StaticVariables.DATA_SOURCES_GLOB_PATH) + glob.glob(StaticVariables.STAGE_STATE_PATH)\
                 + glob.glob(StaticVariables.CONFIGURATION_INIT_PATH) + glob.glob(StaticVariables.FUNCTIONS_PICKLE_GLOB_PATH)\
                 + glob.glob(StaticVariables.MAP_SHUFFLE_HANDLER_PATH) + glob.glob(StaticVariables.STAGE_CONFIGURATION_PATH)\
                 + glob.glob(StaticVariables.STAGE_TO_PIPELINE_PATH) + glob.glob(StaticVariables.PIPELINE_DEPENDENCIES_PATH)\
                 + glob.glob(StaticVariables.IN_DEGREE_PATH) + glob.glob(StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH)\
                 + glob.glob(StaticVariables.DEFAULT_FUNCTIONS_GLOB_PATH) + glob.glob(StaticVariables.STAGE_PROGRESS_PATH)

    zip_obj = ZipFile(zip_name, 'w')
    for file_path in file_paths:
        zip_obj.write(file_path)
    zip_obj.close()
    # faster to zip with shell exec
    # subprocess.call(["zip", "--version"])
    # subprocess.call(['zip', zip_name] + globed_results)
    # subprocess.call(
    #     ['zip', zip_name] + globed_results + glob.glob(StaticVariables.STATIC_JOB_INFO_PATH)
    #     + glob.glob(StaticVariables.LAMBDA_UTILS_PATH) + glob.glob(StaticVariables.MAP_HANDLER_PATH)
    #     + glob.glob(StaticVariables.REDUCE_HANDLER_PATH) + glob.glob(StaticVariables.JOB_INIT_PATH)
    #     + glob.glob(StaticVariables.UTILS_INIT_PATH) + glob.glob(StaticVariables.PARTITION_PATH)
    #     + glob.glob(StaticVariables.COMBINE_PATH) + glob.glob(StaticVariables.STATIC_INIT_PATH)
    #     + glob.glob(StaticVariables.STATIC_VARIABLES_PATH) + glob.glob(StaticVariables.REDUCE_PATH)
    #     + glob.glob(StaticVariables.INPUT_HANDLER_PATH) + glob.glob(StaticVariables.OUTPUT_HANDLER_PATH)
    #     + glob.glob(StaticVariables.DATA_SOURCES_GLOB_PATH) + glob.glob(StaticVariables.STAGE_STATE_PATH)
    #     + glob.glob(StaticVariables.CONFIGURATION_INIT_PATH) + glob.glob(StaticVariables.FUNCTIONS_PICKLE_GLOB_PATH)
    #     + glob.glob(StaticVariables.MAP_SHUFFLE_HANDLER_PATH) + glob.glob(StaticVariables.STAGE_CONFIGURATION_PATH)
    #     + glob.glob(StaticVariables.STAGE_TO_PIPELINE_PATH) + glob.glob(StaticVariables.PIPELINE_DEPENDENCIES_PATH)
    #     + glob.glob(StaticVariables.IN_DEGREE_PATH) + glob.glob(StaticVariables.PIPELINE_TO_FIRST_LAST_STAGE_PATH)
    #     + glob.glob(StaticVariables.DEFAULT_FUNCTIONS_GLOB_PATH) + glob.glob(StaticVariables.STAGE_PROGRESS_PATH)
    # )


def zip_driver_lambda(zip_name, function_filepaths):
    globed_results = []
    for filepath in function_filepaths:
        globed_results += glob.glob(filepath)

    file_paths = globed_results + glob.glob("*") + glob.glob("*/*")
    zip_obj = ZipFile(zip_name, 'w')
    print("Zipping serverless driver code. The filepath are:", file_paths)
    for file_path in file_paths:
        if not file_path.startswith("node_modules"):
            zip_obj.write(file_path)
    zip_obj.close()
    print("Finished zipping serverless driver code")
    # faster to zip with shell exec
    # subprocess.call(['zip', zip_name] + glob.glob("*") + glob.glob("*/*")
    #                 + globed_results)
