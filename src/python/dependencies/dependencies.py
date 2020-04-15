def extract_data_s3(outputs, input_pair):
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
