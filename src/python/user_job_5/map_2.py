def truncate_decimals(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        for ip, revenue in input_value:
            int_revenue = float(revenue)
            outputs.append(tuple((ip, int_revenue)))

    except Exception as e:
        print("type error: " + str(e))
