from serverless_mr.main import ServerlessMR

from user_job_3.map import map_function
from user_job_3.reduce import reduce_function
from user_job_3.partition import partition

serverless_mr = ServerlessMR()
serverless_mr.map(map_function)
serverless_mr.reduce(reduce_function)
serverless_mr.set_partition_function(partition)

serverless_mr.run()
