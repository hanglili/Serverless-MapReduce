import json
import pickle

from serverless_mr.driver.driver import Driver
from serverless_mr.static.static_variables import StaticVariables


def lambda_handler(event, _):
    print("Received event: " + json.dumps(event, indent=2))
    print("Starting the driver")
    with open(StaticVariables.SERVERLESS_PIPELINES_INFO_PATH, 'rb') as f:
        pipelines = pickle.load(f)
    with open(StaticVariables.SERVERLESS_TOTAL_NUM_OPERATIONS_PATH, 'rb') as f:
        total_num_functions = pickle.load(f)
    driver = Driver(pipelines, total_num_functions, is_serverless=True)
    driver.run()
    print("Job executed and Driver shut down")
