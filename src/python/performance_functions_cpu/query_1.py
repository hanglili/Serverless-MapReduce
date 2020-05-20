import time


def map_function(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair
        lines = input_value.split('\n')[:-1]
        page_rank_cutoff = 10

        for line in lines:
            data = line.split(',')
            page_url = data[0]
            page_rank = int(data[1])
            if page_rank > page_rank_cutoff:
                outputs.append(tuple((page_url, page_rank)))
            time.sleep(0.00005)

    except Exception as e:
        print("type error: " + str(e))
