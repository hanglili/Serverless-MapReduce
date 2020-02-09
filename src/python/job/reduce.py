import json

from python.job.reduce_handler import reduce_handler

# Contents are values stored under one S3 key
@reduce_handler
def reduce_function(results, contents):
    line_count = 0
    try:
        for srcIp, val in json.loads(contents).iteritems():
            line_count += 1
            if srcIp not in results:
                results[srcIp] = 0
            results[srcIp] += float(val)
    except Exception as e:
        print("type error: " + str(e))
    return line_count