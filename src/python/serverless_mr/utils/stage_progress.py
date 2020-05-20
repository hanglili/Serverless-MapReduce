import boto3
import json
import os
import decimal
import time
import logging

from static.static_variables import StaticVariables
from utils.setup_logger import logger

logger = logging.getLogger('serverless-mr.stage-progress')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class StageProgress:

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

    def create_progress_table(self, table_name):
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        self.client.create_table(
            AttributeDefinitions=[{
                'AttributeName': 'stage_id',
                'AttributeType': 'N'
            }],
            TableName=table_name,
            KeySchema=[{
                'AttributeName': 'stage_id',
                'KeyType': 'HASH'
            }],
            ProvisionedThroughput={
                'ReadCapacityUnits': 100,
                'WriteCapacityUnits': 100
            }
        )

        # Wait until the created table becomes active
        response = self.client.describe_table(TableName=table_name)['Table']['TableStatus']
        while response != 'ACTIVE':
            time.sleep(1)
            response = self.client.describe_table(TableName=table_name)['Table']['TableStatus']

        logger.info("Stage progress table created successfully")

    def initialise_progress_table(self, table_name, num_stages):
        for i in range(1, num_stages + 1):
            self.client.put_item(
                TableName=table_name,
                Item={
                    'stage_id': {'N': str(i)},
                    'num_processed_keys': {'N': str(0)},
                    'total_num_keys': {'N': str(0)}
                }
            )

        logger.info("Stage progress table initialised successfully")

    def read_progress_table(self, table_name):
        stage_progress = {}
        projection_expression = "stage_id, num_processed_keys, total_num_keys"
        response = self.client.scan(TableName=table_name, ProjectionExpression=projection_expression)
        for record in response['Items']:
            stage_progress[record['stage_id']['N']] = [record['num_processed_keys']['N'],
                                                       record['total_num_keys']['N']]
        return stage_progress

    def delete_progress_table(self, table_name):
        existing_tables = self.client.list_tables()['TableNames']

        if table_name in existing_tables:
            self.client.delete_table(
                TableName=table_name
            )

            print("Stage progress table deleted successfully")
            return

        logger.info("Stage progress table has not been created")

    def update_total_num_keys(self, table_name, stage_id, total_num_keys):
        response = self.client.update_item(
            TableName=table_name,
            Key={
                'stage_id': {'N': str(stage_id)}
            },
            UpdateExpression="set total_num_keys = :val",
            ExpressionAttributeValues={
                ':val': {'N': str(total_num_keys)}
            },
            ReturnValues="UPDATED_NEW"
        )

        logger.info("Total number of processed keys updated successfully")
        return response

    def increase_num_processed_keys(self, table_name, stage_id, additional_num_processed_keys):
        if additional_num_processed_keys > 0:
            response = self.client.update_item(
                TableName=table_name,
                Key={
                    'stage_id': {'N': str(stage_id)}
                },
                UpdateExpression="set num_processed_keys = num_processed_keys + :val",
                ExpressionAttributeValues={
                    ':val': {'N': str(additional_num_processed_keys)}
                },
                ReturnValues="UPDATED_NEW"
            )

            logger.info("Number of processed keys increased by %s successfully" % str(additional_num_processed_keys))
            return response
        return {}
