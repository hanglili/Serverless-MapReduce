from job.map_handler import map_handler


@map_handler
def map_function(output, contents):
    line_count = 0
    for line in contents.decode("utf-8").split('\n')[:-1]:
        line_count +=1
        try:
            data = line.split(',')
            srcIp = data[0]
            if srcIp not in output:
                output[srcIp] = 0
            output[srcIp] += float(data[3])
        except Exception as e:
            print("type error: " + str(e))
    return line_count