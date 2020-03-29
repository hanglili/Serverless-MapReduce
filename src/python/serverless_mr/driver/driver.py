import boto3
import json
import random
import time
import os

from serverless_mr.utils import lambda_utils, zip
from serverless_mr.aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config
from serverless_mr.static.static_variables import StaticVariables


class Driver:

    def __init__(self, is_serverless=False):
        self.config = json.loads(open(StaticVariables.DRIVER_CONFIG_PATH, 'r').read())
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        self.is_serverless = is_serverless
        if self.static_job_info['localTesting']:
            if not self.is_serverless:
                endpoint_url = 'http://localhost:4572'
            else:
                endpoint_url = 'http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME']
            self.s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                                          region_name=StaticVariables.DEFAULT_REGION, endpoint_url=endpoint_url)
        else:
            self.s3_client = boto3.client('s3')
        self.lambda_config = None
        self.lambda_client = None

    # Get all keys to be processed
    def _get_all_keys(self):

        # init
        bucket = self.static_job_info["bucket"]
        region = self.config["region"]
        lambda_memory = self.config["lambdaMemory"]
        concurrent_lambdas = self.config["concurrentLambdas"]
        lambda_read_timeout = self.config["lambdaReadTimeout"]
        boto_max_connections = self.config["botoMaxConnections"]

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        self.lambda_config = Config(read_timeout=lambda_read_timeout,
                                    max_pool_connections=boto_max_connections,
                                    region_name=region)

        if self.static_job_info['localTesting']:
            if not self.is_serverless:
                endpoint_url = 'http://localhost:4574'
            else:
                endpoint_url = 'http://%s:4574' % os.environ['LOCALSTACK_HOSTNAME']
            self.lambda_client = boto3.client('lambda', aws_access_key_id='', aws_secret_access_key='',
                                              region_name=StaticVariables.DEFAULT_REGION,
                                              endpoint_url=endpoint_url, config=self.lambda_config)
        else:
            self.lambda_client = boto3.client('lambda', config=self.lambda_config)

        # Fetch all the keys that match the prefix
        all_keys = []
        for obj in self.s3_client.list_objects(Bucket=bucket, Prefix=self.static_job_info["prefix"])['Contents']:
            if not obj['Key'].endswith('/'):
                print("The object is ", obj)
                all_keys.append(obj)

        print("The number of keys: ", len(all_keys))
        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
        print("The batch size is: ", bsize)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        print("The number of batches is the number of mappers: ", len(batches))
        num_mappers = len(batches)

        return all_keys, num_mappers, batches

    # Create the aws_lambda functions
    def _create_lambda(self, num_mappers):
        # Lambda functions
        job_id = self.static_job_info["jobId"]
        lambda_name_prefix = self.static_job_info["lambdaNamePrefix"]
        mapper_lambda_name = lambda_name_prefix + "-mapper-" + job_id
        reducer_lambda_name = lambda_name_prefix + "-reducer-" + job_id
        rc_lambda_name = lambda_name_prefix + "-rc-" + job_id

        job_bucket = self.config["jobBucket"]
        region = self.config["region"]
        num_reducers = self.config["numReducers"]

        # Prepare Lambda functions if driver running in local machine
        if not self.is_serverless:
            zip.zip_lambda(self.config["mapper"]["name"], self.config["mapper"]["zip"])
            zip.zip_lambda(self.config["reducer"]["name"], self.config["reducer"]["zip"])
            zip.zip_lambda(self.config["reducerCoordinator"]["name"], self.config["reducerCoordinator"]["zip"])

        # Mapper
        l_mapper = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                self.config["mapper"]["zip"], job_id,
                                                mapper_lambda_name, self.config["mapper"]["handler"])
        l_mapper.update_code_or_create_on_no_exist()

        # Reducer
        l_reducer = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                 self.config["reducer"]["zip"], job_id,
                                                 reducer_lambda_name, self.config["reducer"]["handler"])
        l_reducer.update_code_or_create_on_no_exist()

        # Coordinator
        l_rc = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                            self.config["reducerCoordinator"]["zip"], job_id,
                                            rc_lambda_name, self.config["reducerCoordinator"]["handler"])
        l_rc.update_code_or_create_on_no_exist(str(num_mappers))

        # Add permission to the coordinator
        l_rc.add_lambda_permission(random.randint(1, 1000), job_bucket)

        # create event source for coordinator
        last_bin_path = "%s/%sbin%s/" % (job_id, StaticVariables.MAP_OUTPUT_PREFIX, str(num_reducers))
        l_rc.create_s3_event_source_notification(job_bucket, last_bin_path)
        return l_mapper, l_reducer, l_rc

    # Write Jobdata to S3
    def _write_job_data(self, all_keys, n_mappers):
        # xray_recorder.begin_subsegment('Write job data to S3')
        job_id = self.static_job_info["jobId"]
        j_key = "%s/%s" % (job_id, StaticVariables.JOB_DATA_S3_FILENAME)
        data = json.dumps({
            "mapCount": n_mappers,
            "totalS3Files": len(all_keys),
            "startTime": time.time()
        })

        # Write job data to S3
        self.s3_client.put_object(Bucket=self.config["jobBucket"], Key=j_key, Body=data, Metadata={})
        # access_s3.write_to_s3(self.config["jobBucket"], j_key, data, {})

    def invoke_lambda(self, mapper_outputs, batches, m_id):
        """
        aws_lambda invoke function
        """
        bucket = self.static_job_info["bucket"]
        job_bucket = self.config["jobBucket"]
        job_id = self.static_job_info["jobId"]
        lambda_name_prefix = self.static_job_info["lambdaNamePrefix"]
        mapper_lambda_name = lambda_name_prefix + "-mapper-" + job_id

        batch = [k['Key'] for k in batches[m_id - 1]]
        resp = self.lambda_client.invoke(
            FunctionName=mapper_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "bucket": bucket,
                "keys": batch,
                "jobBucket": job_bucket,
                "jobId": job_id,
                "mapperId": m_id
            })
        )
        out = eval(resp['Payload'].read())
        mapper_outputs.append(out)
        print("Mapper processing information: ", out)

    def _invoke_mappers(self, num_mappers, batches):
        mapper_outputs = []
        concurrent_lambdas = self.config["concurrentLambdas"]

        # Exec Parallel
        print("Number of Mappers: ", num_mappers)
        pool = ThreadPool(num_mappers)
        ids = [i + 1 for i in range(num_mappers)]
        invoke_lambda_partial = partial(self.invoke_lambda, mapper_outputs, batches)

        # Burst request handling
        mappers_executed = 0
        while mappers_executed < num_mappers:
            nm = min(concurrent_lambdas, num_mappers)
            results = pool.map(invoke_lambda_partial, ids[mappers_executed: mappers_executed + nm])
            mappers_executed += nm

        pool.close()
        pool.join()

        print("All the mappers have finished")
        return mapper_outputs

    def _calculate_cost(self, mapper_outputs):
        total_lambda_secs = 0
        total_s3_get_ops = 0
        # total_s3_put_ops = 0
        # s3_storage_hours = 0
        total_lines = 0

        for output in mapper_outputs:
            total_s3_get_ops += int(output[0])
            total_lines += int(output[1])
            total_lambda_secs += float(output[2])

        # Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs
        # Get all reducer keys

        # Total execution time for reducers
        reducer_lambda_time = 0
        job_bucket = self.config["jobBucket"]
        lambda_memory = self.config["lambdaMemory"]
        job_id = self.static_job_info["jobId"]
        output_bucket = job_bucket if self.static_job_info["outputBucket"] == "" else self.static_job_info["outputBucket"]
        output_prefix = self.static_job_info["outputPrefix"]

        reduce_output_full_prefix = \
            "%s/%s" % (job_id, StaticVariables.REDUCE_OUTPUT_PREFIX) if output_prefix == "" else output_prefix

        while True:
            response = self.s3_client.list_objects(Bucket=output_bucket, Prefix=reduce_output_full_prefix)
            if "Contents" in response:
                job_keys = response["Contents"]
                print("Checking whether the job is completed ...")
                # check job done
                if len(job_keys) == self.config["numReducers"]:
                    print("Job Complete")
                    keys = [jk["Key"] for jk in job_keys]
                    total_s3_size = sum([jk["Size"] for jk in job_keys])
                    for key in keys:
                        # Even though metadata processing time is written as processingTime, AWS does not accept
                        # uppercase letter metadata key
                        reducer_lambda_time += float(self.s3_client.get_object(Bucket=job_bucket, Key=key)
                                                     ['Metadata']['processingtime'])
                        # reducer_lambda_time += float(self.s3.Object(job_bucket, key).metadata['processingtime'])
                    break

            time.sleep(5)

        # S3 Storage cost - Account for mappers only; This cost is neglibile anyways since S3
        # costs 3 cents/GB/month
        s3_storage_hour_cost = 1 * 0.0000521574022522109 * (
                total_s3_size / 1024.0 / 1024.0 / 1024.0)  # cost per GB/hr
        s3_put_cost = len(job_keys) * 0.005 / 1000

        # S3 GET # $0.004/10000
        total_s3_get_ops += len(job_keys)
        s3_get_cost = total_s3_get_ops * 0.004 / 10000

        # Total Lambda costs
        total_lambda_secs += reducer_lambda_time
        lambda_cost = total_lambda_secs * 0.00001667 * lambda_memory / 1024.0
        s3_cost = (s3_get_cost + s3_put_cost + s3_storage_hour_cost)

        # Print costs
        print("Reducer L", reducer_lambda_time * 0.00001667 * lambda_memory / 1024.0)
        print("Lambda Cost", lambda_cost)
        print("S3 Storage Cost", s3_storage_hour_cost)
        print("S3 Request Cost", s3_get_cost + s3_put_cost)
        print("S3 Cost", s3_cost)
        print("Total Cost: ", lambda_cost + s3_cost)
        print("Total Lines:", total_lines)

    def run(self):
        # 1. Get all keys to be processed
        all_keys, num_mappers, batches = self._get_all_keys()

        # 2. Create the aws_lambda functions
        l_mapper, l_reducer, l_rc = self._create_lambda(num_mappers)
        self._write_job_data(all_keys, num_mappers)

        # Execute
        # 3. Invoke Mappers and wait until they finish the execution
        mapper_outputs = self._invoke_mappers(num_mappers, batches)

        # 4. Delete Mapper function
        l_mapper.delete_function()

        # 5. Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        self._calculate_cost(mapper_outputs)

        # 6. Delete Reducer and its coordinator function
        l_reducer.delete_function()
        l_rc.delete_function()
