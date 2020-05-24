import boto3
import os
import time
import logging

from static.static_variables import StaticVariables

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.input-handler-dynamodb')

id_cnt = 1


class InputHandlerDynamoDB:

    def __init__(self, in_lambda, is_local_testing):
        if is_local_testing:
            if in_lambda:
                local_endpoint_url = 'http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4569'
            self.client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('dynamodb')

    @staticmethod
    def create_table(client, table_name, input_partition_key, input_sort_key):
        if input_sort_key is None:
            client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': input_partition_key[0],
                        'AttributeType': input_partition_key[1]
                    }
                ],
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': input_partition_key[0],
                        'KeyType': 'HASH'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        else:
            client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': input_partition_key[0],
                        'AttributeType': input_partition_key[1]
                    },
                    {
                        'AttributeName': input_sort_key[0],
                        'AttributeType': input_sort_key[1]
                    }
                ],
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': input_partition_key[0],
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': input_sort_key[0],
                        'KeyType': 'RANGE'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )

        # Wait until the created table becomes active

        response = client.describe_table(TableName=table_name)['Table']['TableStatus']
        while response != 'ACTIVE':
            time.sleep(1)
            response = client.describe_table(TableName=table_name)['Table']['TableStatus']


    @staticmethod
    def put_items(client, table_name, filepath, input_partition_key, input_sort_key, input_columns):
        global id_cnt
        with open(filepath) as fp:
            line = fp.readline()
            while line:
                items = {}
                items[input_partition_key[0]] = {
                    input_partition_key[1]: str(id_cnt)
                }
                if input_sort_key is not None:
                    items[input_sort_key[0]] = {
                        input_sort_key[1]: str(id_cnt)
                    }

                column_values = line.rstrip().split(',')
                assert len(column_values) == len(input_columns)

                for i in range(len(input_columns)):
                    input_column = input_columns[i]
                    items[input_column[0]] = {
                        input_column[1]: column_values[i]
                    }

                client.put_item(
                    TableName=table_name,
                    # For local testing, input_partition_key and input_sort_key are assumed to be same
                    Item=items
                )
                line = fp.readline()
                id_cnt += 1

    def set_up_local_input_data(self, input_filepaths, static_job_info):
        input_partition_key = static_job_info[StaticVariables.INPUT_PARTITION_KEY_DYNAMODB]
        input_sort_key = static_job_info[StaticVariables.INPUT_SORT_KEY_DYNAMODB] \
            if StaticVariables.INPUT_SORT_KEY_DYNAMODB in static_job_info else None
        input_columns = static_job_info[StaticVariables.INPUT_COLUMNS_DYNAMODB]
        input_table_name = static_job_info[StaticVariables.INPUT_SOURCE_FN]
        InputHandlerDynamoDB.create_table(self.client, input_table_name, input_partition_key, input_sort_key)

        for input_filepath in input_filepaths:
            if os.path.isdir(input_filepath):
                continue
            InputHandlerDynamoDB.put_items(self.client, input_table_name, input_filepath,
                                           input_partition_key, input_sort_key, input_columns)

        logger.info("Set up local input data successfully")

    def get_all_input_keys(self, static_job_info):
        # Returns all input keys to be processed: a list of format obj where obj is a map of {'Key': ..., 'Size': ...}
        all_keys = []
        input_table_name = static_job_info[StaticVariables.INPUT_SOURCE_FN]
        response = self.client.describe_table(TableName=input_table_name)
        number_of_records = response['Table']['ItemCount']
        if number_of_records == 0:
            logger.warning("Number of live records in DynamoDB is 0. "
                           "Note that DynamoDB updates this number every 6 hours.")
            logger.info("The average size of one record will be assumed to be maximum")
            # The maximum size of a DynamoDB item is 400KB.
            one_record_avg_size = 400 * 1024
        else:
            one_record_avg_size = response['Table']['TableSizeBytes'] / number_of_records

        input_partition_key = static_job_info[StaticVariables.INPUT_PARTITION_KEY_DYNAMODB]
        input_sort_key = static_job_info[StaticVariables.INPUT_SORT_KEY_DYNAMODB] \
            if StaticVariables.INPUT_SORT_KEY_DYNAMODB in static_job_info else None

        projection_expression = input_partition_key[0]
        if input_sort_key is not None:
            projection_expression += ",%s" % input_sort_key[0]

        response = self.client.scan(TableName=input_table_name, ProjectionExpression=projection_expression)
        for record in response['Items']:
            record_key = {input_partition_key[0]: record[input_partition_key[0]][input_partition_key[1]]}
            if input_sort_key is not None:
                record_key[input_sort_key[0]] = record[input_sort_key[0]][input_sort_key[1]]

            all_keys.append({'Key': record_key, 'Size': int(one_record_avg_size)})

        return all_keys

    def read_value(self, input_source, input_key, static_job_info):
        input_table_name = input_source
        input_partition_key = static_job_info[StaticVariables.INPUT_PARTITION_KEY_DYNAMODB]
        input_sort_key = static_job_info[StaticVariables.INPUT_SORT_KEY_DYNAMODB] \
            if StaticVariables.INPUT_SORT_KEY_DYNAMODB in static_job_info else None
        input_processing_columns = static_job_info[StaticVariables.INPUT_PROCESSING_COLUMNS_DYNAMODB]

        input_processing_column_names = []
        for input_processing_column in input_processing_columns:
            input_processing_column_names.append(input_processing_column[0])

        projection_expression = ",".join(input_processing_column_names)

        input_partition_key_name = input_partition_key[0]
        input_partition_key_type = input_partition_key[1]

        if input_sort_key is None:
            response = self.client.get_item(TableName=input_table_name,
                                            Key={
                                                input_partition_key_name: {
                                                    input_partition_key_type: input_key[input_partition_key_name]
                                                }
                                            },
                                            ProjectionExpression=projection_expression)
        else:
            input_sort_key_name = input_sort_key[0]
            input_sort_key_type = input_sort_key[1]
            response = self.client.get_item(TableName=input_table_name,
                                            Key={
                                                input_partition_key_name: {
                                                    input_partition_key_type: input_key[input_partition_key_name]
                                                },
                                                input_sort_key_name: {
                                                    input_sort_key_type: input_key[input_sort_key_name]
                                                }
                                            },
                                            ProjectionExpression=projection_expression)

        record_value = {}
        for input_processing_column in input_processing_columns:
            input_processing_column_name = input_processing_column[0]
            input_processing_column_type = input_processing_column[1]
            record_value[input_processing_column_name] = \
                response['Item'][input_processing_column_name][input_processing_column_type]

        return record_value
