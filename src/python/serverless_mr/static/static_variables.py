class StaticVariables:

    # constants
    # STATIC_JOB_INFO_PATH = find_filepath("configuration", "static-job-info.json")
    STATIC_JOB_INFO_PATH = "serverless_mr/configuration/static-job-info.json"
    # CONFIGURATION_INIT_PATH = find_filepath("configuration", "__init__.py")
    CONFIGURATION_INIT_PATH = "serverless_mr/configuration/__init__.py"
    # JOB_INFO_LAMBDA_PATH = "/tmp/job-info.json"
    MAP_HANDLER_PATH = "serverless_mr/job/map_handler.py"
    REDUCE_HANDLER_PATH = "serverless_mr/job/reduce_handler.py"
    PARTITION_PATH = "serverless_mr/job/partition.py"
    REDUCE_PATH = "serverless_mr/job/reduce.py"
    COMBINE_PATH = "serverless_mr/job/combine.py"
    UTILS_INIT_PATH = "serverless_mr/job/__init__.py"
    JOB_INIT_PATH = "serverless_mr/utils/__init__.py"
    LAMBDA_UTILS_PATH = "serverless_mr/utils/lambda_utils.py"
    MAP_OUTPUT_PREFIX = "task/mapper/"
    REDUCE_OUTPUT_PREFIX = "task/reducer/"
    # DRIVER_CONFIG_PATH = find_filepath("configuration", "driver.json")
    DRIVER_CONFIG_PATH = "serverless_mr/configuration/driver.json"
    JOB_DATA_S3_FILENAME = "jobdata"
    STATIC_VARIABLES_PATH = "serverless_mr/static/static_variables.py"
    STATIC_INIT_PATH = "serverless_mr/static/__init__.py"

    DEFAULT_REGION = "us-east-1"
    LAMBDA_MEMORY_LIMIT = 1536
    LAMBDA_TIMEOUT = 300
