import time


def map_function_2(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        time.sleep(40)
        for src_ip, value in input_value:
            outputs.append(tuple((src_ip, value)))

    except Exception as e:
        print("Error: " + str(e))


def reduce_function_2(outputs, intermediate_data):
    """
    :param outputs: (k2, [v2]) where k2 and v2 are output data
    which can be of any types
    :param intermediate_data: (k2, [v2]) where k2 and v2 are of type string.
    Users need to convert them to their respective types explicitly.
    NOTE: intermediate data type is the same as the output data type
    """
    key, values = intermediate_data

    try:
        sum_ad_revenue = 0
        sum_page_rank = 0
        for value in values:
            sum_ad_revenue += value[1]
            sum_page_rank += value[0]

        avg_page_rank = sum_page_rank / len(values)
        if len(outputs) == 0:
            outputs.append(tuple((key, [avg_page_rank, sum_ad_revenue])))
        elif outputs[0][1][1] < sum_ad_revenue:
            outputs[0] = tuple((key, [avg_page_rank, sum_ad_revenue]))

        time.sleep(0.0005)

    except Exception as e:
        print("Error: " + str(e))

