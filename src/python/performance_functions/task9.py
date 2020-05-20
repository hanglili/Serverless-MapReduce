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
            data = line.split(',')
            src_ip = data[0]
            duration = int(data[8]) + 6
            outputs.append(tuple((src_ip, fib(duration))))

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

    duration_sum = 0
    try:
        for value in values:
            duration_sum += value

        magic_number = fib((duration_sum % 10) + 5)
        outputs.append(tuple((key, magic_number)))
    except Exception as e:
        print("type error: " + str(e))

