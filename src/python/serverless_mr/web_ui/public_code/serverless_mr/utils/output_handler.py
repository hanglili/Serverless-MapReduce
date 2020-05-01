from data_sources.output_handler_dynamodb import OutputHandlerDynamoDB
from data_sources.output_handler_s3 import OutputHandlerS3


def get_output_handler(output_source_type, is_local_testing, in_lambda=False):
    if output_source_type == "s3":
        return OutputHandlerS3(in_lambda=in_lambda, is_local_testing=is_local_testing)
    elif output_source_type == "dynamodb":
        return OutputHandlerDynamoDB(in_lambda=in_lambda, is_local_testing=is_local_testing)
    else:
        raise ValueError("Error! Input Source Type not recognised")
