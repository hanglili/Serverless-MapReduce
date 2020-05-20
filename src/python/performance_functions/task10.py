import re


url_regex = re.compile("(?P<url>https?://[^\s]+)")


def fib(n):
    if n < 0:
        print("Incorrect input")
        return 0
    elif n == 1:
        return 0
    elif n == 2:
        return 1
    else:
        return fib(n-1) + fib(n-2)


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
                i = 0
                while i < 10000:
                    i += 1

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
            in_link_count += value

        i = 0
        while i < 10000:
            i += 1
        outputs.append(tuple((key, in_link_count)))
    except Exception as e:
        print("type error: " + str(e))

