import glob
import subprocess

from serverless_mr.static.static_variables import StaticVariables


def zip_lambda(filenames, zip_name):
    globed_results = []
    for filename in filenames:
        globed_results += glob.glob(filename)
    # faster to zip with shell exec
    subprocess.call(
        ['zip', zip_name] + globed_results + glob.glob(StaticVariables.STATIC_JOB_INFO_PATH)
        + glob.glob(StaticVariables.LAMBDA_UTILS_PATH) + glob.glob(StaticVariables.MAP_HANDLER_PATH)
        + glob.glob(StaticVariables.REDUCE_HANDLER_PATH) + glob.glob(StaticVariables.JOB_INIT_PATH)
        + glob.glob(StaticVariables.UTILS_INIT_PATH) + glob.glob(StaticVariables.PARTITION_PATH)
        + glob.glob(StaticVariables.COMBINE_PATH) + glob.glob(StaticVariables.STATIC_INIT_PATH)
        + glob.glob(StaticVariables.STATIC_VARIABLES_PATH) + glob.glob(StaticVariables.REDUCE_PATH)
        + glob.glob(StaticVariables.INPUT_HANDLER_PATH) + glob.glob(StaticVariables.OUTPUT_HANDLER_PATH)
        + glob.glob(StaticVariables.DATA_SOURCES_GLOB_PATH) + glob.glob(StaticVariables.MAPPER_PHASE_STATE_PATH)
        + glob.glob(StaticVariables.CONFIGURATION_INIT_PATH) + glob.glob("serverless_mr/job/*.pkl")
    )


def zip_driver_lambda(zip_name):
    # faster to zip with shell exec
    project_name = "serverless_mr"
    subprocess.call(['zip', zip_name] + glob.glob("%s/*" % project_name) + glob.glob("%s/*/*" % project_name))
