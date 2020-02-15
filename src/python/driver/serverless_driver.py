import json

from driver.driver import Driver


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("Starting the driver")

    driver = Driver()
    driver.run()

    print("Job executed and Driver shut down")


