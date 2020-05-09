def map_function_3(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        for src_ip, value in input_value:
            outputs.append(tuple((src_ip, value)))

    except Exception as e:
        print("Error: " + str(e))


def reduce_function_3(outputs, intermediate_data):
    """
    :param outputs: (k2, [v2]) where k2 and v2 are output data
    which can be of any types
    :param intermediate_data: (k2, [v2]) where k2 and v2 are of type string.
    Users need to convert them to their respective types explicitly.
    NOTE: intermediate data type is the same as the output data type
    """
    key, values = intermediate_data

    try:
        assert len(values) == 1
        if len(outputs) == 0:
            outputs.append(tuple((key, values[0])))
        elif outputs[0][1][1] < values[0][1]:
            outputs[0] = tuple((key, values[0]))

    except Exception as e:
        print("Error: " + str(e))

