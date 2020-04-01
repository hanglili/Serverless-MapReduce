import boto3
import json
import os

from serverless_mr.static.static_variables import StaticVariables


class OutputHandlerDynamoDB:

    def __init__(self, in_lambda):
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
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
    def create_table(client, table_name):
        try:
            client.create_table(
                AttributeDefinitions=[{
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }],
                TableName=table_name,
                KeySchema=[{
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        except client.exceptions.ResourceInUseException as e:
            print("Metadata table has already been created")

    @staticmethod
    def put_items(client, table_name, data):
        for pair in data:
            response = client.put_item(
                TableName=table_name,
                Item={
                    'id': {'S': str(pair[0])},
                    'line': {'S': str(pair[1])}
                }
            )

    @staticmethod
    def put_metadata(client, metadata_table_name, metadata, output_table_name):
        response = client.put_item(
            TableName=metadata_table_name,
            Item={
                'id': {'S': str(output_table_name)},
                'metadata': {'S': str(metadata)}
            }
        )

    def write_output(self, reducer_id, outputs, metadata):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        reduce_output_full_prefix = "%s-%s" % (job_name, StaticVariables.REDUCE_OUTPUT_PREFIX_DYNAMODB) \
            if StaticVariables.OUTPUT_PREFIX_FN not in self.static_job_info \
            else self.static_job_info[StaticVariables.OUTPUT_PREFIX_FN]

        metadata_table_name = "metadata"
        output_table_name = "%s-%s" % (reduce_output_full_prefix, reducer_id)

        OutputHandlerDynamoDB.create_table(self.client, output_table_name)
        OutputHandlerDynamoDB.create_table(self.client, metadata_table_name)

        OutputHandlerDynamoDB.put_items(self.client, output_table_name, outputs)
        OutputHandlerDynamoDB.put_metadata(self.client, metadata_table_name, json.dumps(metadata), output_table_name)

    def list_objects_for_checking_finish(self):
        metadata_table_name = "metadata"
        existing_tables = self.client.list_tables()['TableNames']
        if metadata_table_name in existing_tables:
            response = self.client.scan(TableName=metadata_table_name, ProjectionExpression='id, metadata')
            return response, "Items"

        return {}, metadata_table_name

    def check_job_finish(self, response, string_index):
        job_keys = []
        job_metadata = []
        reducer_lambda_time = 0

        for record in response[string_index]:
            job_keys.append(record['id']['S'])
            job_metadata.append(json.loads(record['metadata']['S']))

        if len(job_keys) == self.static_job_info[StaticVariables.NUM_REDUCER_FN]:
            total_s3_size = 0
            for job_metadatum in job_metadata:
                # Even though metadata processing time is written as processingTime,
                # AWS does not accept uppercase letter metadata key
                reducer_lambda_time += float(job_metadatum['processingTime'])
                total_s3_size += float(job_metadatum['lineCount'])
            return reducer_lambda_time, total_s3_size, len(job_keys)

        return -1, -1, -1

    def get_output(self, reducer_id):
        job_name = self.static_job_info[StaticVariables.JOB_NAME_FN]
        reduce_output_full_prefix = "%s-%s" % (job_name, StaticVariables.REDUCE_OUTPUT_PREFIX_DYNAMODB) \
            if StaticVariables.OUTPUT_PREFIX_FN not in self.static_job_info \
            else self.static_job_info[StaticVariables.OUTPUT_PREFIX_FN]

        output_table_name = "%s-%s" % (reduce_output_full_prefix, reducer_id)

        outputs = []
        response = self.client.scan(TableName=output_table_name)
        for record in response['Items']:
            output = (record['id']['S'], record['line']['S'])
            outputs.append(output)

        return outputs