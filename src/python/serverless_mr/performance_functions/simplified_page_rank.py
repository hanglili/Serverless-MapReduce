import re


url_regex = re.compile("(?P<url>https?://[^\s]+)")


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
            for url in url_regex.findall(line):
                outputs.append(tuple((url, 1)))

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

    in_link_count = 0
    try:
        for value in values:
            in_link_count += float(value)

        outputs.append(tuple((key, in_link_count)))
    except Exception as e:
        print("type error: " + str(e))

