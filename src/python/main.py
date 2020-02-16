import sys

from driver.driver import Driver
from driver.serverless_driver_setup import ServerlessDriverSetup


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Wrong number of arguments.")
    else:
        mode = sys.argv[1]

        if int(mode) == 0:
            driver = Driver()
            driver.run()
        else:
            serverless_driver_setup = ServerlessDriverSetup()
            serverless_driver_setup.register_driver()
            print("Driver Lambda function successfully registered")
            command = input("Enter invoke to invoke and other keys to exit: ")
            if command == "invoke":
                print("Driver invoked and starting job execution")
                serverless_driver_setup.invoke()
