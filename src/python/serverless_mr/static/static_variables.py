class StaticVariables:

    # File locations
    # STATIC_JOB_INFO_PATH = find_filepath("configuration", "static-job-info.json")
    STATIC_JOB_INFO_PATH = "configuration/static-job-info.json"
    # CONFIGURATION_INIT_PATH = find_filepath("configuration", "__init__.py")
    CONFIGURATION_INIT_PATH = "configuration/__init__.py"
    SERVERLESS_PIPELINES_INFO_PATH = "configuration/serverless-pipelines-info.pkl"
    SERVERLESS_TOTAL_NUM_OPERATIONS_PATH = "configuration/serverless-total-num-operations.pkl"
    STAGE_CONFIGURATION_PATH = "configuration/stage-config.json"
    PIPELINE_DEPENDENCIES_PATH = "configuration/pipeline-dependencies.json"
    STAGE_TO_PIPELINE_PATH = "configuration/stage-to-pipeline.json"
    PIPELINE_TO_FIRST_LAST_STAGE_PATH = "configuration/pipeline-to-first-last-stage.json"
    # JOB_INFO_LAMBDA_PATH = "/tmp/job-info.json"
    MAP_HANDLER_PATH = "job/map_handler.py"
    MAP_SHUFFLE_HANDLER_PATH = "job/map_shuffle_handler.py"
    REDUCE_HANDLER_PATH = "job/reduce_handler.py"
    COORDINATOR_HANDLER_PATH = "coordinator/coordinator.py"
    PARTITION_PATH = "job/partition.py"
    REDUCE_PATH = "job/reduce.py"
    COMBINE_PATH = "job/combine.py"
    JOB_INIT_PATH = "job/__init__.py"
    FUNCTIONS_PICKLE_GLOB_PATH = "job/*.pkl"
    DEFAULT_FUNCTIONS_GLOB_PATH = "default/*.py"
    DEFAULT_PARTITION_FUNCTION_PATH = "default/partition.py"
    UTILS_INIT_PATH = "utils/__init__.py"
    LAMBDA_UTILS_PATH = "utils/lambda_utils.py"
    INPUT_HANDLER_PATH = "utils/input_handler.py"
    OUTPUT_HANDLER_PATH = "utils/output_handler.py"
    DATA_SOURCES_GLOB_PATH = "data_sources/*.py"
    # DRIVER_CONFIG_PATH = find_filepath("configuration", "driver.json")
    DRIVER_CONFIG_PATH = "configuration/driver.json"
    JOB_DATA_S3_FILENAME = "jobdata"
    STATIC_VARIABLES_PATH = "static/static_variables.py"
    STATIC_INIT_PATH = "static/__init__.py"
    OUTPUT_PREFIX = "stage"
    REDUCE_OUTPUT_PREFIX_S3 = "stage"
    IN_DEGREE_DYNAMODB_TABLE_NAME = "%s-in-degree"
    IN_DEGREE_PATH = "utils/in_degree.py"
    STAGE_STATE_DYNAMODB_TABLE_NAME = "%s-stage-state"
    STAGE_STATE_PATH = "utils/stage_state.py"

    # S3 file locations - Web UI
    S3_JOBS_INFORMATION_BUCKET_NAME = "serverless-mapreduce-job-information"
    S3_UI_GENERAL_JOB_INFORMATION_PATH = "web-ui/%s/static-job-info.json"
    S3_UI_STAGE_CONFIGURATION_PATH = "web-ui/%s/stage-config.json"
    S3_UI_DAG_INFORMATION_PATH = "web-ui/%s/dag-information.json"
    S3_UI_REGISTERED_JOB_INFORMATION_PATH = "web-ui/%s/registered-job-info.json"
    S3_UI_REGISTERED_JOB_DRIVER_CONFIG_PATH = "web-ui/%s/driver.json"

    # DynamoDB table names - Web UI
    STAGE_PROGRESS_DYNAMODB_TABLE_NAME = "%s-stage-progress"
    STAGE_PROGRESS_PATH = "utils/stage_progress.py"

    # Lambda handler paths
    MAP_HANDLER_FUNCTION_PATH = "job/map_handler.lambda_handler"
    MAP_SHUFFLE_HANDLER_FUNCTION_PATH = "job/map_shuffle_handler.lambda_handler"
    REDUCE_HANDLER_FUNCTION_PATH = "job/reduce_handler.lambda_handler"
    COORDINATOR_HANDLER_FUNCTION_PATH = "coordinator/coordinator.lambda_handler"
    SERVERLESS_DRIVER_HANDLER_FUNCTION_PATH = "driver/serverless_driver.lambda_handler"

    # Zip path
    COORDINATOR_ZIP_PATH = "coordinator.zip"
    DRIVER_ZIP_PATH = "serverless_driver.zip"
    LAMBDA_ZIP_GLOB_PATH = "*.zip"

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
