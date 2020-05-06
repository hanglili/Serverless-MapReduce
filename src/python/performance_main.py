from serverless_mr.main import ServerlessMR

from performance_functions.query_2_map import map_function
from performance_functions.query_2_reduce import reduce_function

serverless_mr = ServerlessMR()
serverless_mr.map(map_function).reduce(reduce_function, 50).run()

