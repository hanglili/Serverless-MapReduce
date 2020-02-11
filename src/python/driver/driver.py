import boto3
import json
import random
import time
import logging

from utils import access_s3, data_encoding, lambda_utils
from aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


class Driver:
    JOB_ID = "bl-release"
    DRIVER_CONFIG_PATH = "configuration/driver.json"
    L_PREFIX = "BL"
    TASK_MAPPER_PREFIX = JOB_ID + "/task/mapper/"
    TASK_REDUCER_PREFIX = JOB_ID + "/task/reducer/"

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.lambda_config = None
        self.lambda_client = None
        self.config = json.loads(open(Driver.DRIVER_CONFIG_PATH, 'r').read())

    @staticmethod
    def _setting():
        patch_all()
        logging.basicConfig(level='WARNING')
        logging.getLogger('aws_xray_sdk').setLevel(logging.ERROR)

        # collect all tracing samples
        sampling_rules = {"version": 1, "default": {"fixed_target": 1, "rate": 1}}
        xray_recorder.configure(sampling_rules=sampling_rules)

    # Get all keys to be processed
    def _get_all_keys(self):
        xray_recorder.begin_subsegment('Get all keys to be processed')

        # init
        bucket = self.config["bucket"]
        region = self.config["region"]
        lambda_memory = self.config["lambdaMemory"]
        concurrent_lambdas = self.config["concurrentLambdas"]
        lambda_read_timeout = self.config["lambda_read_timeout"]
        boto_max_connections = self.config["boto_max_connections"]

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        self.lambda_config = Config(read_timeout=lambda_read_timeout,
                                    max_pool_connections=boto_max_connections,
                                    region_name=region)
        self.lambda_client = boto3.client('lambda', config=self.lambda_config)

        # Fetch all the keys that match the prefix
        all_keys = []
        for obj in self.s3.Bucket(bucket).objects.filter(Prefix=self.config["prefix"]).all():
            all_keys.append(obj)

        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        n_mappers = len(batches)
        document = xray_recorder.current_subsegment()
        document.put_metadata("Batch size: ", bsize, "Processing initialization")
        document.put_metadata("Mappers: ", n_mappers, "Processing initialization")
        xray_recorder.end_subsegment()  # Get all keys to be processed
        return all_keys, n_mappers, batches

    # Create the aws_lambda functions
    def _create_lambda(self, num_mappers):
        xray_recorder.begin_subsegment('Prepare Lambda functions')

        # Lambda functions
        mapper_lambda_name = Driver.L_PREFIX + "-mapper-" + Driver.JOB_ID
        reducer_lambda_name = Driver.L_PREFIX + "-reducer-" + Driver.JOB_ID
        rc_lambda_name = Driver.L_PREFIX + "-rc-" + Driver.JOB_ID
        job_bucket = self.config["jobBucket"]
        region = self.config["region"]
        num_reducers = self.config["num_reducers"]

        # write job self.config
        access_s3.write_job_config(Driver.JOB_ID, job_bucket, num_mappers, reducer_lambda_name,
                                   self.config["reducer"]["handler"], num_reducers)

        # Prepare Lambda functions
        data_encoding.zip_lambda(self.config["mapper"]["name"], self.config["mapper"]["zip"])
        data_encoding.zip_lambda(self.config["reducer"]["name"], self.config["reducer"]["zip"])
        data_encoding.zip_lambda(self.config["reducerCoordinator"]["name"], self.config["reducerCoordinator"]["zip"])
        xray_recorder.end_subsegment()

        # Mapper
        xray_recorder.begin_subsegment('Create mapper Lambda function')
        l_mapper = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                self.config["mapper"]["zip"], Driver.JOB_ID,
                                                mapper_lambda_name, self.config["mapper"]["handler"])
        l_mapper.update_code_or_create_on_noexist()
        xray_recorder.end_subsegment()  # Create mapper Lambda function

        # Reducer
        xray_recorder.begin_subsegment('Create reducer Lambda function')
        l_reducer = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                                 self.config["reducer"]["zip"], Driver.JOB_ID,
                                                 reducer_lambda_name, self.config["reducer"]["handler"])
        l_reducer.update_code_or_create_on_noexist()
        xray_recorder.end_subsegment()  # Create reducer Lambda function

        # Coordinator
        xray_recorder.begin_subsegment('Create reducer coordinator Lambda function')
        l_rc = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                            self.config["reducerCoordinator"]["zip"], Driver.JOB_ID,
                                            rc_lambda_name, self.config["reducerCoordinator"]["handler"])
        l_rc.update_code_or_create_on_noexist()

        # Add permission to the coordinator
        l_rc.add_lambda_permission(random.randint(1, 1000), job_bucket)

        # create event source for coordinator
        l_rc.create_s3_eventsource_notification(job_bucket, Driver.TASK_MAPPER_PREFIX + "bin" + str(num_reducers) + "/")
        xray_recorder.end_subsegment()  # Create reducer coordinator Lambda function
        return l_mapper, l_reducer, l_rc

    # Write Jobdata to S3
    def _write_job_data(self, all_keys, n_mappers):
        xray_recorder.begin_subsegment('Write job data to S3')
        j_key = Driver.JOB_ID + "/jobdata"
        data = json.dumps({
            "mapCount": n_mappers,
            "totalS3Files": len(all_keys),
            "startTime": time.time()
        })

        xray_recorder.current_subsegment().put_metadata("Job data: ", data, "Write job data to S3")
        # Write job data to S3
        access_s3.write_to_s3(self.config["jobBucket"], j_key, data, {})
        xray_recorder.end_subsegment()

    def invoke_lambda(self, mapper_outputs, batches, m_id):
        bucket = self.config["bucket"]
        xray_recorder.begin_segment('Invoke mapper Lambda')
        '''
        aws_lambda invoke function
        '''
        job_bucket = self.config["jobBucket"]
        mapper_lambda_name = Driver.L_PREFIX + "-mapper-" + Driver.JOB_ID

        # batch = [k['Key'] for k in batches[m_id-1]]
        batch = [k.key for k in batches[m_id - 1]]
        xray_recorder.current_segment().put_annotation("batch_for_mapper_" + str(m_id), str(batch))
        # print "invoking", m_id, len(batch)
        resp = self.lambda_client.invoke(
            FunctionName=mapper_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "bucket": bucket,
                "keys": batch,
                "jobBucket": job_bucket,
                "jobId": Driver.JOB_ID,
                "mapperId": m_id
            })
        )
        out = eval(resp['Payload'].read())
        mapper_outputs.append(out)
        print("mapper output", out)
        xray_recorder.end_segment()

    def _invoke_mappers(self, n_mappers, batches):
        mapper_outputs = []
        xray_recorder.begin_subsegment('Invoke mappers')

        concurrent_lambdas = self.config["concurrentLambdas"]

        # Exec Parallel
        print("# of Mappers ", n_mappers)
        pool = ThreadPool(n_mappers)
        Ids = [i + 1 for i in range(n_mappers)]
        invoke_lambda_partial = partial(self.invoke_lambda, mapper_outputs, batches)

        # Burst request handling
        mappers_executed = 0
        while mappers_executed < n_mappers:
            nm = min(concurrent_lambdas, n_mappers)
            results = pool.map(invoke_lambda_partial, Ids[mappers_executed: mappers_executed + nm])
            mappers_executed += nm
            xray_recorder.current_subsegment().put_metadata("Mapper lambdas executed: ", mappers_executed,
                                                            "Invoke mappers")

        pool.close()
        pool.join()

        print("all the mappers finished")
        xray_recorder.end_subsegment()
        return mapper_outputs

    def _calculate_cost(self, mapper_outputs):
        xray_recorder.begin_subsegment('Calculate cost')

        total_lambda_secs = 0
        total_s3_get_ops = 0
        total_s3_put_ops = 0
        s3_storage_hours = 0
        total_lines = 0

        for output in mapper_outputs:
            total_s3_get_ops += int(output[0])
            total_lines += int(output[1])
            total_lambda_secs += float(output[2])

        # Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs
        # Get all reducer keys
        reducer_keys = []

        # Total execution time for reducers
        reducer_lambda_time = 0
        job_bucket = self.config["jobBucket"]
        lambda_memory = self.config["lambdaMemory"]

        while True:
            response = self.s3_client.list_objects(Bucket=job_bucket, Prefix=Driver.TASK_REDUCER_PREFIX)
            if "Contents" in response:
                job_keys = response["Contents"]
                keys = [jk["Key"] for jk in job_keys]
                total_s3_size = sum([jk["Size"] for jk in job_keys])

                print("check to see if the job is done")

                # check job done
                if len(job_keys) == 8:
                    print("job done")
                    # reducer_lambda_time += float(
                    #     self.s3.Object(job_bucket, Driver.JOB_ID + "/result").metadata['processingtime'])
                    # for key in keys:
                    #     if "task/reducer" in key:
                    #         reducer_lambda_time += float(self.s3.Object(job_bucket, key).metadata['processingtime'])
                    #         reducer_keys.append(key)
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
        xray_recorder.end_subsegment()  # Calculate cost


    def run(self):
        self._setting()

        xray_recorder.begin_segment('MapReduce Driver')

        # 1. Get all keys to be processed
        all_keys, n_mappers, batches = self._get_all_keys()

        # 2. Create the aws_lambda functions
        l_mapper, l_reducer, l_rc = self._create_lambda(n_mappers)
        self._write_job_data(all_keys, n_mappers)

        ### Execute ###
        # 3. Invoke Mappers and wait until they finish the execution
        mapper_outputs = self._invoke_mappers(n_mappers, batches)

        # 4. Delete Mapper function
        xray_recorder.begin_subsegment('Delete mappers')
        l_mapper.delete_function()
        xray_recorder.end_subsegment()  # Delete mappers

        # 5. Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        self._calculate_cost(mapper_outputs)

        # 6. Delete Reducer and its coordinator function
        xray_recorder.begin_subsegment('Delete reducers')
        l_reducer.delete_function()
        l_rc.delete_function()
        xray_recorder.end_subsegment()  # Delete reducers

        xray_recorder.end_segment()  # MapReduce Driver
