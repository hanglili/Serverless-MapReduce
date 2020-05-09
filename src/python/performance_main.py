from serverless_mr.main import ServerlessMR

serverless_mr = ServerlessMR()

# query 1
# from performance_functions.query_1 import map_function
# serverless_mr.map(map_function).run()
#
# query 2
# from performance_functions.query_2 import map_function, reduce_function
# serverless_mr.map(map_function).reduce(reduce_function, 4).run()
#
# query 2b
# from performance_functions.query_2_var import map_function, reduce_function
# serverless_mr.map(map_function).reduce(reduce_function, 4).run()
#
# query 3
from performance_functions.query_3_phase_1 import map_function_1, reduce_function_1
from performance_functions.query_3_phase_2 import map_function_2, reduce_function_2
from performance_functions.query_3_phase_3 import map_function_3, reduce_function_3
serverless_mr.map(map_function_1).reduce(reduce_function_1, 4)
serverless_mr.map(map_function_2).reduce(reduce_function_2, 4)
serverless_mr.map(map_function_3).reduce(reduce_function_3, 1)
serverless_mr.run()

# Simplified Page Rank
# from performance_functions.simplified_page_rank import map_function, reduce_function
# serverless_mr.map(map_function).reduce(reduce_function, 4).run()

# Sort
# from performance_functions.sorting import map_function, reduce_function
# from performance_functions.range_partition import partition
# serverless_mr.map(map_function).shuffle(partition).reduce(reduce_function, 4).run()
