def extract_data_dynamo_db(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair
        src_ip = input_value['sourceIP']
        ad_revenue = float(input_value['adRevenue'])
        outputs.append(tuple((src_ip, ad_revenue)))
    except Exception as e:
        print("type error: " + str(e))


def extract_data_s3(outputs, input_pair):
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


def identity_function(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        for ip, revenue in input_value:
            outputs.append(tuple((ip, revenue)))

    except Exception as e:
        print("type error: " + str(e))


import hashlib


def partition(key, num_bins):
    """
    :param key: key of intermediate data which can be of any type
    :param num_bins: number of bins that a key could be partitioned to (this is equal to number of reducers)
    :returns an integer that denotes the bin assigned to this key
    """
    key_hashed = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % 10**8
    return key_hashed % num_bins


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


def remove_dots(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        for ip, revenue in input_value:
            src_ip = ip.replace(".", "")
            outputs.append(tuple((src_ip, revenue)))

    except Exception as e:
        print("type error: " + str(e))


def truncate_to_four_chars(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair

        for ip, revenue in input_value:
            src_ip = ip[:4]
            outputs.append(tuple((src_ip, revenue)))

    except Exception as e:
        print("type error: " + str(e))
