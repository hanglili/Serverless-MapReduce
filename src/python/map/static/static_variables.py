class StaticVariables:

    # constants
    STATIC_JOB_INFO_PATH = 'configuration/static-job-info.json'
    # JOB_INFO_LAMBDA_PATH = "/tmp/job-info.json"
    MAP_HANDLER_PATH = "job/map_handler.py"
    REDUCE_HANDLER_PATH = "job/reduce_handler.py"
    PARTITION_PATH = "job/partition.py"
    REDUCE_PATH = "job/reduce.py"
    COMBINE_PATH = "job/combine.py"
    UTILS_INIT_PATH = "job/__init__.py"
    JOB_INIT_PATH = "utils/__init__.py"
    LAMBDA_UTILS_PATH = "utils/lambda_utils.py"
    MAP_OUTPUT_PREFIX = "task/mapper/"
    REDUCE_OUTPUT_PREFIX = "task/reducer/"
    DRIVER_CONFIG_PATH = "configuration/driver.json"
    JOB_DATA_S3_FILENAME = "jobdata"
    STATIC_VARIABLES_PATH = "static/static_variables.py"
    STATIC_INIT_PATH = "static/__init__.py"

    DEFAULT_REGION = "us-east-1"
    LAMBDA_MEMORY_LIMIT = 1536
    LAMBDA_TIMEOUT = 300
