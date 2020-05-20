import datetime
import time


def map_function_1(outputs, input_pair):
    """
    :param outputs: [(k2, v2)] where k2 and v2 are intermediate data
    which can be of any types
    :param input_pair: (k1, v1) where k1 and v1 are assumed to be of type string
    """
    try:
        _, input_value = input_pair
        lines = input_value.split('\n')[:-1]
        cut_off_date = datetime.datetime.strptime('2000-01-01', "%Y-%m-%d").date()

        time.sleep(30)
        fields = lines[0].split(',')
        # Ranking file
        if len(fields) == 3:
            for line in lines:
                data = line.split(',')
                page_url = data[0]
                page_rank = int(data[1])

                outputs.append(tuple((page_url, {'recordType': 'ranking', 'pageRank': page_rank})))
        # Uservisits file
        elif len(fields) == 9:
            for line in lines:
                data = line.split(',')
                src_ip = data[0]
                dst_url = data[1]
                ad_revenue = float(data[3])
                date = datetime.datetime.strptime(data[2], "%Y-%m-%d").date()
                if date < cut_off_date:
                    outputs.append(tuple((dst_url, {'recordType': 'visits', 'srcIp': src_ip, 'adRevenue': ad_revenue})))
        else:
            print("Invalid record: %s" % str(fields))

    except Exception as e:
        print("Error: " + str(e))


def reduce_function_1(outputs, intermediate_data):
    """
    :param outputs: (k2, [v2]) where k2 and v2 are output data
    which can be of any types
    :param intermediate_data: (k2, [v2]) where k2 and v2 are of type string.
    Users need to convert them to their respective types explicitly.
    NOTE: intermediate data type is the same as the output data type
    """
    key, values = intermediate_data
    ranking_value = None
    buffered_visit_values = []

    # URL is a primary key in the Ranking table, hence it is unique. For each URL, there will only be one entry in the
    # Ranking table.
    try:
        for value in values:
            if value['recordType'] == 'ranking':
                ranking_value = value
                for visit_value in buffered_visit_values:
                    outputs.append(tuple((visit_value['srcIp'],
                                          [ranking_value['pageRank'], visit_value['adRevenue']])))
                buffered_visit_values = []
            elif ranking_value is not None:
                outputs.append(tuple((value['srcIp'],
                                      [ranking_value['pageRank'], value['adRevenue']])))
            else:
                buffered_visit_values.append(value)

        time.sleep(0.0005)

    except Exception as e:
        print("Error: " + str(e))

