from serverless_mr.data_sources.output_handler_dynamodb import OutputHandlerDynamoDB
from serverless_mr.data_sources.output_handler_s3 import OutputHandlerS3


def get_output_handler(output_source_type, in_lambda=False):
    if output_source_type == "s3":
        return OutputHandlerS3(in_lambda)
    elif output_source_type == "dynamodb":
        return OutputHandlerDynamoDB(in_lambda)
    else:
        raise ValueError("Error! Input Source Type not recognised")
