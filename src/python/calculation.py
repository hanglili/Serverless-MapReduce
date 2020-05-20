lambda_memory = 3008
num_mappers = 10
num_reducers = 20
average_execution_time_mapper = 20.71820330619812
average_execution_time_reducer = 1.8371081948280334
total_lambda_cost = 0.023982938996838333
total_cost = 0.0252048795990549


total_lambda_time = total_lambda_cost * 1024.0 / (0.00001667 * lambda_memory)
print("The old total lambda time: %s" % total_lambda_time)

total_coordinator_time = total_lambda_time - (2 * num_mappers * average_execution_time_mapper)\
                         - (2 * num_reducers * average_execution_time_reducer)
print("The total coordinator execution time: %s" % total_coordinator_time)

new_total_lambda_time = total_coordinator_time + num_mappers * average_execution_time_mapper\
                        + num_reducers * average_execution_time_reducer
print("The new total lambda time: %s" % new_total_lambda_time)

new_total_lambda_cost = new_total_lambda_time * 0.00001667 * lambda_memory / 1024.0
print("The new total lambda cost: %s" % new_total_lambda_cost)

print("New total cost: %s" % (total_cost - total_lambda_cost + new_total_lambda_cost))

