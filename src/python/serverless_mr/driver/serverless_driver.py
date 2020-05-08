import json
import pickle
import os
import logging

from driver.driver import Driver
from static.static_variables import StaticVariables

tmp_static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        if tmp_static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            root.setLevel(level=logging.INFO)
        else:
            root.removeHandler(handler)

from utils.setup_logger import logger
logger = logging.getLogger('serverless-mr.serverless-driver')


def lambda_handler(event, _):
    logger.info("Received event: " + json.dumps(event, indent=2))
    logger.info("*********************Serverless Driver***************************")
    with open(StaticVariables.SERVERLESS_PIPELINES_INFO_PATH, 'rb') as f:
        pipelines = pickle.load(f)
    total_num_functions = int(os.environ.get("total_num_stages"))
    driver = Driver(pipelines, total_num_functions, is_serverless=True)
    driver.run()
    logger.info("Job executed and Driver shut down")
