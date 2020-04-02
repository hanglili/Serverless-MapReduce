from serverless_mr.job.map_handler import map_handler

import re


@map_handler
def map_function(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, line = input_pair
        words = re.split('; |, |\*|\n| |:|\.', line)
        for word in words:
            outputs.append(tuple((word, 1)))
    except Exception as e:
        print("type error: " + str(e))

