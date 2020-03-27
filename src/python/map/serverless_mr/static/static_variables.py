import pkg_resources


class StaticVariables:

    # constants
    STATIC_JOB_INFO_PATH = pkg_resources.resource_filename("configuration", "static-job-info.json")
    # JOB_INFO_LAMBDA_PATH = "/tmp/job-info.json"
    MAP_HANDLER_PATH = "serverless_mr/job/map_handler.py"
    REDUCE_HANDLER_PATH = "serverless_mr/job/reduce_handler.py"
    PARTITION_PATH = "serverless_mr/job/partition.py"
    REDUCE_PATH = "serverless_mr/job/reduce.py"
    COMBINE_PATH = "serverless_mr/job/combine.py"
    UTILS_INIT_PATH = "serverless_mr/job/__init__.py"
    JOB_INIT_PATH = "serverless_mr/utils/__init__.py"
    LAMBDA_UTILS_PATH = "serverless_mr/utils/lambda_utils.py"
    MAP_OUTPUT_PREFIX = "serverless_mr/task/mapper/"
    REDUCE_OUTPUT_PREFIX = "serverless_mr/task/reducer/"
    DRIVER_CONFIG_PATH = pkg_resources.resource_filename("configuration", "driver.json")
    JOB_DATA_S3_FILENAME = "serverless_mr/jobdata"
    STATIC_VARIABLES_PATH = "serverless_mr/static/static_variables.py"
    STATIC_INIT_PATH = "serverless_mr/static/__init__.py"

    DEFAULT_REGION = "us-east-1"
    LAMBDA_MEMORY_LIMIT = 1536
    LAMBDA_TIMEOUT = 300
