from data_sources.input_handler_dynamodb import InputHandlerDynamoDB
from data_sources.input_handler_s3 import InputHandlerS3


def get_input_handler(input_source_type, is_local_testing, in_lambda=False):
    if input_source_type == "s3":
        return InputHandlerS3(in_lambda, is_local_testing)
    elif input_source_type == "dynamodb":
        return InputHandlerDynamoDB(in_lambda, is_local_testing)
    else:
        raise ValueError("Error! Input Source Type not recognised")
