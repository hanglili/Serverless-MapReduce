from serverless_mr.main import ServerlessMR

serverless_mr = ServerlessMR()

# query 1
# from performance_functions_cpu.query_1 import map_function
# serverless_mr.map(map_function).run()


# query 2b
# from performance_functions_cpu.query_2_var import map_function, reduce_function
# serverless_mr.map(map_function).reduce(reduce_function, 15).run()

# query 3
from performance_functions_cpu.query_3_phase_1 import map_function_1, reduce_function_1
from performance_functions_cpu.query_3_phase_2 import map_function_2, reduce_function_2
from performance_functions_cpu.query_3_phase_3 import map_function_3, reduce_function_3
serverless_mr.map(map_function_1).reduce(reduce_function_1, 30)
serverless_mr.map(map_function_2).reduce(reduce_function_2, 30)
serverless_mr.map(map_function_3).reduce(reduce_function_3, 1)
serverless_mr.run()
