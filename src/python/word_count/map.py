import re


def produce_counts(outputs, input_pair):
    try:
        _, input_value = input_pair
        lines = input_value.split('\n')[:-1]

        for line in lines:
            words = re.split('; |, |\*|\n| |:|\.', line)
            for word in words:
                outputs.append(tuple((word, 1)))
    except Exception as e:
        print("type error: " + str(e))

