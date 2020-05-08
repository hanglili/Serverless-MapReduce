def map_function(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair
        lines = input_value.split('\n')[:-1]

        for line in lines:
            data = line.split(',')
            src_ip = data[0]
            ad_revenue = float(data[3])
            outputs.append(tuple((src_ip, ad_revenue)))

    except Exception as e:
        print("type error: " + str(e))

def reduce_function(outputs, intermediate_data):
    """
    :param outputs: (k2, [v2]) where k2 and v2 are output data
    which can be of any types
    :param intermediate_data: (k2, [v2]) where k2 and v2 are of type string.
    Users need to convert them to their respective types explicitly.
    NOTE: intermediate data type is the same as the output data type
    """
    key, values = intermediate_data

    revenue_sum = 0
    try:
        for value in values:
            revenue_sum += float(value)

        outputs.append(tuple((key, revenue_sum)))
    except Exception as e:
        print("type error: " + str(e))

