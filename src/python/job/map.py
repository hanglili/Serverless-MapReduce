from job.map_handler import map_handler


@map_handler
def map_function(outputs, input_pair):
    """
    :param output: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param line: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, line = input_pair
        data = line.split(',')
        src_ip = data[0]
        ad_revenue = float(data[3])
        outputs.append((src_ip, ad_revenue))
    except Exception as e:
        print("type error: " + str(e))
