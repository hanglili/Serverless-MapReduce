from serverless_mr.main import ServerlessMR

from user_job_5.map import extract_data
from user_job_5.map_2 import truncate_decimals
from user_job_5.reduce import reduce_function
from user_job_5.partition import partition
from user_job_5.map_3 import remove_dots
from user_job_5.map_4 import truncate_to_four_chars

serverless_mr = ServerlessMR()
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition)\
#     .reduce(reduce_function, 4).map(remove_dots).map(truncate_to_four_chars)
# serverless_mr.map_shuffle(extract_data, partition).reduce(reduce_function, 4)
# serverless_mr.map(extract_data).map(truncate_decimals)
# serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition).reduce(reduce_function, 4)
serverless_mr.map(extract_data).map_shuffle(truncate_decimals, partition)\
    .reduce(reduce_function, 4).map(remove_dots).map_shuffle(truncate_to_four_chars, partition)\
    .reduce(reduce_function, 3)

serverless_mr.run()
