import re


def produce_counts(outputs, input_pair):
    try:
        _, input_value = input_pair
        # Split input_value (a document) into lines
        lines = input_value.split('\n')[:-1]

        for line in lines:
            # For each line, split it into words
            words = re.split('; |, |\*|\n| |:|\.', line)
            for word in words:
                # For each word occurrence, add a tuple (word, 1) to outputs
                outputs.append(tuple((word, 1)))
    except Exception as e:
        print("type error: " + str(e))

