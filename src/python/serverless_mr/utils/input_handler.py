from serverless_mr.data_sources.input_handler_dynamodb import InputHandlerDynamoDB
from serverless_mr.data_sources.input_handler_s3 import InputHandlerS3


def get_input_handler(input_source_type, in_lambda=False):
    if input_source_type == "s3":
        return InputHandlerS3(in_lambda)
    elif input_source_type == "dynamodb":
        return InputHandlerDynamoDB(in_lambda)
    else:
        raise ValueError("Error! Input Source Type not recognised")
