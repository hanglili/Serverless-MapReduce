import json

from serverless_mr.driver.driver import Driver


def lambda_handler(event, _):
    print("Received event: " + json.dumps(event, indent=2))
    print("Starting the driver")

    driver = Driver(is_serverless=True)
    driver.run()

    print("Job executed and Driver shut down")
