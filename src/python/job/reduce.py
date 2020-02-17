from job.reduce_handler import reduce_handler

# Contents are values stored under one S3 key
@reduce_handler
def reduce_function(outputs, intermediate_data):
    """
    :param outputs: (k2, [v2]) where k2 and v2 are output data
    which can be of any types
    :param intermediate_data: (k2, [v2]) where k2 and v2 are of type string.
    Users need to convert them to their respective types explicitly.
    NOTE: intermediate data type is the same as the output data type
    """
    # TODO: Change revenue to [revenue]
    key, values = intermediate_data

    revenue_sum = 0
    try:
        for value in values:
            revenue_sum += float(value)

        outputs.append((key, revenue_sum))
    except Exception as e:
        print("type error: " + str(e))
