class StaticVariables:

    # File locations
    # STATIC_JOB_INFO_PATH = find_filepath("configuration", "static-job-info.json")
    STATIC_JOB_INFO_PATH = "serverless_mr/configuration/static-job-info.json"
    # CONFIGURATION_INIT_PATH = find_filepath("configuration", "__init__.py")
    CONFIGURATION_INIT_PATH = "serverless_mr/configuration/__init__.py"
    SERVERLESS_PIPELINES_INFO_PATH = "serverless_mr/configuration/serverless-pipelines-info.pkl"
    SERVERLESS_TOTAL_NUM_OPERATIONS_PATH = "serverless_mr/configuration/serverless-total-num-operations.pkl"
    STAGE_CONFIGURATION_PATH = "serverless_mr/configuration/stage-config.json"
    PIPELINE_DEPENDENCIES_PATH = "serverless_mr/configuration/pipeline-dependencies.json"
    STAGE_TO_PIPELINE_PATH = "serverless_mr/configuration/stage-to-pipeline.json"
    PIPELINE_TO_FIRST_LAST_STAGE_PATH = "serverless_mr/configuration/pipeline-to-first-last-stage.json"
    # JOB_INFO_LAMBDA_PATH = "/tmp/job-info.json"
    MAP_HANDLER_PATH = "serverless_mr/job/map_handler.py"
    MAP_SHUFFLE_HANDLER_PATH = "serverless_mr/job/map_shuffle_handler.py"
    REDUCE_HANDLER_PATH = "serverless_mr/job/reduce_handler.py"
    COORDINATOR_HANDLER_PATH = "serverless_mr/coordinator/coordinator.py"
    PARTITION_PATH = "serverless_mr/job/partition.py"
    REDUCE_PATH = "serverless_mr/job/reduce.py"
    COMBINE_PATH = "serverless_mr/job/combine.py"
    JOB_INIT_PATH = "serverless_mr/job/__init__.py"
    FUNCTIONS_PICKLE_GLOB_PATH = "serverless_mr/job/*.pkl"
    DEFAULT_FUNCTIONS_GLOB_PATH = "serverless_mr/default/*.py"
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
    OUTPUT_PREFIX = "stage"
    REDUCE_OUTPUT_PREFIX_S3 = "stage"
    IN_DEGREE_DYNAMODB_TABLE_NAME = "in-degree"
    IN_DEGREE_PATH = "serverless_mr/utils/in_degree.py"
    STAGE_STATE_DYNAMODB_TABLE_NAME = "stage-state"
    STAGE_STATE_PATH = "serverless_mr/utils/stage_state.py"

    # Lambda handler paths
    MAP_HANDLER_FUNCTION_PATH = "serverless_mr/job/map_handler.lambda_handler"
    MAP_SHUFFLE_HANDLER_FUNCTION_PATH = "serverless_mr/job/map_shuffle_handler.lambda_handler"
    REDUCE_HANDLER_FUNCTION_PATH = "serverless_mr/job/reduce_handler.lambda_handler"
    COORDINATOR_HANDLER_FUNCTION_PATH = "serverless_mr/coordinator/coordinator.lambda_handler"
    SERVERLESS_DRIVER_HANDLER_FUNCTION_PATH = "serverless_mr/driver/serverless_driver.lambda_handler"

    # Zip path
    COORDINATOR_ZIP_PATH = "serverless_mr/coordinator.zip"
    DRIVER_ZIP_PATH = "serverless_mr/serverless_driver.zip"
    LAMBDA_ZIP_GLOB_PATH = "serverless_mr/*.zip"

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
    INPUT_PARTITION_KEY_DYNAMODB = "inputPartitionKeyDynamoDB"
    INPUT_SORT_KEY_DYNAMODB = "inputSortKeyDynamoDB" # Optional
    INPUT_COLUMNS_DYNAMODB = "inputColumnsDynamoDB"
    INPUT_PROCESSING_COLUMNS_DYNAMODB = "inputProcessingColumnsDynamoDB"
    OUTPUT_PARTITION_KEY_DYNAMODB = "outputPartitionKeyDynamoDB"
    OUTPUT_COLUMN_DYNAMODB = "outputColumnDynamoDB"

    # dynamic variables
    PROJECT_WORKING_DIRECTORY = ""
    LIBRARY_WORKING_DIRECTORY = ""
