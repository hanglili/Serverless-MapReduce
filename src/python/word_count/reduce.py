
def aggregate_counts(outputs, intermediate_data):
    key, values = intermediate_data

    sum_counts = 0
    try:
        for value in values:
            sum_counts += value

        outputs.append(tuple((key, sum_counts)))
    except Exception as e:
        print("type error: " + str(e))
