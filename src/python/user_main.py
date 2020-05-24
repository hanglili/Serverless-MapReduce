from serverless_mr.main import ServerlessMR

from user_job_5.map import extract_data_s3
# from user_job_5.map_2 import truncate_decimals
# from user_job_5.map_3 import remove_dots
# from user_job_5.map_4 import truncate_to_four_chars
# from user_job_5.partition import partition
from user_job_5.reduce import reduce_function

# from user_job_6.map import extract_data_dynamo_db
# from user_job_6.map_2 import extract_data_s3
# from user_job_6.reduce import reduce_function
# from user_job_6.partition import partition
# from user_job_6.map_3 import identity_function


# from user_functions.functions import extract_data_dynamo_db
# from user_functions.functions import extract_data_s3
# from user_functions.functions import reduce_function
# from user_functions.functions import partition
# from user_functions.functions import identity_function


serverless_mr = ServerlessMR()
serverless_mr.map(extract_data_s3).reduce(reduce_function, 4).run()
# serverless_mr.map(extract_data_s3).map(truncate_decimals).combine(reduce_function).shuffle(partition)\
#     .reduce(reduce_function, 4).map(remove_dots).map(truncate_to_four_chars).run()

# serverless_mr.map(extract_data).map(truncate_decimals)\
#     .reduce(reduce_function, 4).map(remove_dots).map(truncate_to_four_chars).run()
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition)\
#     .reduce(reduce_function, 4).map(remove_dots).map(truncate_to_four_chars).run()
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition).reduce(reduce_function, 4).run()
# serverless_mr.map(extract_data).map(truncate_decimals).run()
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition).reduce(reduce_function, 4)
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition)\
#     .reduce(reduce_function, 4).map(remove_dots).map_shuffle(truncate_to_four_chars, partition)\
#     .reduce(reduce_function, 3).run()

config_pipeline_1 = {
    "inputSourceType": "s3",
    "inputSource": "serverless-mapreduce-input-storage",
    "inputPrefix": "testing_partitioned",
    "localTestingInputPath": "../../input_data/testing_partitioned/s3/"
}

online_config_pipeline_1 = {
    "inputSourceType": "s3",
    "inputSource": "serverless-mapreduce-storage",
    "inputPrefix": "testing_partitioned",
}

config_pipeline_2 = {
    "inputSourceType": "dynamodb",
    "inputSource": "serverless-mapreduce-input-storage",
    "inputPartitionKeyDynamoDB": ["recordId", "N"],
    "inputProcessingColumnsDynamoDB": [["sourceIP", "S"], ["adRevenue", "N"]],
    "inputColumnsDynamoDB": [
        ["sourceIP", "S"], ["destURL", "S"], ["visitDate", "S"], ["adRevenue", "N"], ["userAgent","S"],
        ["countryCode", "S"], ["languageCode", "S"], ["searchWord", "S"], ["duration", "S"]
    ],
    "localTestingInputPath": "../../input_data/testing_partitioned/dynamodb/"
}

# config_pipeline_3 = {
#     "outputSourceType": "s3",
#     "outputSource": "serverless-mapreduce-storage-output",
#     "outputPrefix": "output"
# }

# serverless_mr = ServerlessMR()
# pipeline1 = serverless_mr.config(config_pipeline_1).map(extract_data_s3).combine(reduce_function)\
#     .reduce(reduce_function, 4).finish()
#
# pipeline2 = serverless_mr.config(config_pipeline_2).map(extract_data_dynamo_db)\
#     .reduce(reduce_function, 2).finish()
#
# pipeline3 = serverless_mr.config({}).merge([pipeline1, pipeline2]).map(identity_function)\
#     .combine(reduce_function).shuffle(partition).reduce(reduce_function, 5).run()

# pipeline3 = serverless_mr.config(config_pipeline_3).merge([pipeline1, pipeline2]).map(identity_function).run()
