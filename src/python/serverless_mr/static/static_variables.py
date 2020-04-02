class StaticVariables:

    # File locations
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
    JOB_INIT_PATH = "serverless_mr/job/__init__.py"
    UTILS_INIT_PATH = "serverless_mr/utils/__init__.py"
    LAMBDA_UTILS_PATH = "serverless_mr/utils/lambda_utils.py"
    INPUT_HANDLER_PATH = "serverless_mr/utils/input_handler.py"
    OUTPUT_HANDLER_PATH = "serverless_mr/utils/output_handler.py"
    DATA_SOURCES_GLOB_PATH = "serverless_mr/data_sources/*.py"
    # DRIVER_CONFIG_PATH = find_filepath("configuration", "driver.json")
    DRIVER_CONFIG_PATH = "serverless_mr/configuration/driver.json"
    JOB_DATA_S3_FILENAME = "jobdata"
    STATIC_VARIABLES_PATH = "serverless_mr/static/static_variables.py"
    STATIC_INIT_PATH = "serverless_mr/static/__init__.py"
    MAP_OUTPUT_PREFIX = "task/mapper"
    REDUCE_OUTPUT_PREFIX_S3 = "task/reducer"
    REDUCE_OUTPUT_PREFIX_DYNAMODB = "task-reducer"
    MAPPER_PHASE_STATE_DYNAMODB_TABLE_NAME = "mapper-phase-state"
    MAPPER_PHASE_STATE_PATH = "serverless_mr/utils/map_phase_state.py"


    # Constants
    DEFAULT_REGION = "us-east-1"
    DEFAULT_LAMBDA_MEMORY_LIMIT = 1536
    DEFAULT_LAMBDA_TIMEOUT = 300
    DEFAULT_LAMBDA_READ_TIMEOUT = 300
    DEFAULT_BOTO_MAX_CONNECTIONS = 1000
    DEFAULT_NUM_CONCURRENT_LAMBDAS = 1000


    # Naming of config files (FN stands for field name)
    # driver.json
    REGION_FN = "region"
    LAMBDA_MEMORY_PROVISIONED_FN = "lambdaMemoryProvisioned"
    NUM_CONCURRENT_LAMBDAS_FN = "concurrentLambdas"
    LAMBDA_READ_TIMEOUT_FN = "lambdaReadTimeout"
    BOTO_MAX_CONNECTIONS_FN = "botoMaxConnections"
    MAPPER_FN = "mapper"
    REDUCER_FN = "reducer"
    REDUCER_COORDINATOR_FN = "reducerCoordinator"
    DRIVER_FN = "driver"
    LOCATION_FN = "location"
    HANDLER_FN = "handler"
    ZIP_FN = "zip"

    # static-job-info-.json
    JOB_NAME_FN = "jobName"
    SHUFFLING_BUCKET_FN = "shufflingBucket"
    NUM_REDUCER_FN = "numReducers"
    LAMBDA_NAME_PREFIX_FN = "lambdaNamePrefix"
    INPUT_SOURCE_TYPE_FN = "inputSourceType"
    INPUT_SOURCE_FN = "inputSource"
    INPUT_PREFIX_FN = "inputPrefix"
    OUTPUT_SOURCE_TYPE_FN = "outputSourceType"
    OUTPUT_SOURCE_FN = "outputSource"
    OUTPUT_PREFIX_FN = "outputPrefix"
    USE_COMBINE_FLAG_FN = "useCombine"
    LOCAL_TESTING_FLAG_FN = "localTesting"
    LOCAL_TESTING_INPUT_PATH = "localTestingInputPath"
    SERVERLESS_DRIVER_FLAG_FN = "serverlessDriver"

    # specific to DynamoDB
    INPUT_KEY_NAME_DYNAMODB = "inputKeyNameDynamoDB"
    INPUT_COLUMN_NAME_DYNAMODB = "inputColumnNameDynamoDB"
    OUTPUT_KEY_NAME_DYNAMODB = "outputKeyNameDynamoDB"
    OUTPUT_COLUMN_NAME_DYNAMODB = "outputColumnNameDynamoDB"
