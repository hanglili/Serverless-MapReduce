import boto3
import json
import os
import time

from serverless_mr.static.static_variables import StaticVariables


class OutputHandlerDynamoDB:
    METADATA_TABLE_KEY_NAME = "id"
    METADATA_TABLE_COLUMN_NAME = "metadata"

    def __init__(self, in_lambda):
        # S3 client required to calculate the cost of S3 shuffling bucket
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if in_lambda:
                local_endpoint_url = 'http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME']
                s3_local_endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4569'
                s3_local_endpoint_url = 'http://localhost:4572'
            self.client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                       region_name=StaticVariables.DEFAULT_REGION,
                                       endpoint_url=s3_local_endpoint_url)
        else:
            self.client = boto3.client('dynamodb')
            self.s3_client = boto3.client('s3')

    @staticmethod
    def create_table(client, table_name, output_partition_key):
        try:
            client.create_table(
                AttributeDefinitions=[{
                    'AttributeName': output_partition_key[0],
                    'AttributeType': output_partition_key[1]
                }],
                TableName=table_name,
                KeySchema=[{
                    'AttributeName': output_partition_key[0],
                    'KeyType': 'HASH'
                }],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        except client.exceptions.ResourceInUseException as e:
            print("%s table has already been created" % table_name)

        response = client.describe_table(TableName=table_name)['Table']['TableStatus']
        while response != 'ACTIVE':
            time.sleep(1)
            response = client.describe_table(TableName=table_name)['Table']['TableStatus']

    @staticmethod
    def put_items(client, table_name, data, output_partition_key, output_column):
        for output_pair in data:
            response = client.put_item(
                TableName=table_name,
                Item={
                    output_partition_key[0]: {
                        output_partition_key[1]: str(output_pair[0])
                    },
                    output_column[0]: {
                        output_column[1]: str(output_pair[1])
                    }
                }
            )

    @staticmethod
    def put_metadata(client, metadata_table_name, metadata, reducer_id):
        response = client.put_item(
            TableName=metadata_table_name,
            Item={
                OutputHandlerDynamoDB.METADATA_TABLE_KEY_NAME: {'S': str(reducer_id)},
                OutputHandlerDynamoDB.METADATA_TABLE_COLUMN_NAME: {'S': str(metadata)}
            }
        )

    def write_output(self, reducer_id, outputs, metadata):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        metadata_table_name = "%s-metadata" % job_name
        output_table_name = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        output_partition_key = self.static_job_info[StaticVariables.OUTPUT_PARTITION_KEY_DYNAMODB]
        output_column = self.static_job_info[StaticVariables.OUTPUT_COLUMN_DYNAMODB]

        OutputHandlerDynamoDB.create_table(self.client, output_table_name, output_partition_key)
        OutputHandlerDynamoDB.create_table(self.client, metadata_table_name,
                                           [OutputHandlerDynamoDB.METADATA_TABLE_KEY_NAME, 'S'])

        OutputHandlerDynamoDB.put_items(self.client, output_table_name, outputs, output_partition_key, output_column)
        OutputHandlerDynamoDB.put_metadata(self.client, metadata_table_name, json.dumps(metadata), reducer_id)

    def list_objects_for_checking_finish(self):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        metadata_table_name = "%s-metadata" % job_name
        existing_tables = self.client.list_tables()['TableNames']
        project_expression = '%s, %s' % (OutputHandlerDynamoDB.METADATA_TABLE_KEY_NAME,
                                         OutputHandlerDynamoDB.METADATA_TABLE_COLUMN_NAME)

        if metadata_table_name in existing_tables\
                and self.client.describe_table(TableName=metadata_table_name)['Table']['TableStatus'] == 'ACTIVE':
            response = self.client.scan(TableName=metadata_table_name, ProjectionExpression=project_expression)
            return response, "Items"

        return {}, "Items"

    def check_job_finish(self, response, string_index, num_final_dst_operators):
        reducer_ids = []
        reducer_metadata = []
        reducer_lambda_time = 0

        for record in response[string_index]:
            reducer_ids.append(record[OutputHandlerDynamoDB.METADATA_TABLE_KEY_NAME]['S'])
            reducer_metadata.append(json.loads(record[OutputHandlerDynamoDB.METADATA_TABLE_COLUMN_NAME]['S']))

        if len(reducer_ids) == num_final_dst_operators:
            shuffling_bucket = self.static_job_info[StaticVariables.SHUFFLING_BUCKET_FN]
            job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
            job_keys = self.s3_client.list_objects(Bucket=shuffling_bucket, Prefix=job_name)["Contents"]
            total_s3_size = 0
            for metadatum in reducer_metadata:
                # Even though metadata processing time is written as processingTime,
                # AWS does not accept uppercase letter metadata key
                reducer_lambda_time += float(metadatum['processingTime'])
                total_s3_size += float(metadatum['lineCount'])
            return reducer_lambda_time, total_s3_size, len(job_keys)

        return -1, -1, -1

    def get_output(self, reducer_id):
        output_table_name = self.static_job_info[StaticVariables.OUTPUT_SOURCE_FN]

        output_partition_key = self.static_job_info[StaticVariables.OUTPUT_PARTITION_KEY_DYNAMODB]
        output_column = self.static_job_info[StaticVariables.OUTPUT_COLUMN_DYNAMODB]

        outputs = []
        response = self.client.scan(TableName=output_table_name)
        for record in response['Items']:
            output = (record[output_partition_key[0]][output_partition_key[1]],
                      record[output_column[0]][output_column[1]])
            outputs.append(output)

        return outputs
