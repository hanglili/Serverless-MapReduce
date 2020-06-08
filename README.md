# ServerlessMR

ServerlessMR is a big data MapReduce framework that leverages serverless functions to allow processing of large data sets in a 
completely serverless architecture. Currently it supports AWS serverless functions (AWS Lambda). 
ServerlessMR can be seen as a light-weight alternative to Hadoop MapReduce.

Unlike Hadoop MapReduce, ServerlessMR allows users to perform big data processing with no operational effort and 
no prior Hadoop knowledge. Nowadays, Hadoop is one of the most popular big data processing frameworks, however 
it is undeniable that it has a deep learning curve and requires users to manage the underlying infrastructure. 
By minimising the number of components in the framework and abstracting the infrastructure management, ServerlessMR 
simplifies data processing for users, who now can focus more on actual data problems.

Due to its serverless nature, users only get charged for the time that a job is being executed, getting rid of the 
superfluous cost caused by idle periods of the underlying infrastructure used to execute the job. 
Hence, ServerlessMR is particularly-suited to run ad-hoc MapReduce jobs.

ServerlessMR is also very easy to set up, since many of the manual steps required for the traditional data processing 
frameworks have been automated. One such automated step is AWS services deployments with AWS CloudFormation. 

## Architecture
Serverless-MR has the following architecture:
![Architecture](images/Serverless-MR-job-timeline-3.png?raw=true "")

## Key Features
- Offers 2 modes to run any data processing jobs:
    1. Serverful driver: In this mode, the driver runs on the local machine. 
    1. Serverless driver: Completely serverless mode where even the driver is run in AWS Lambda. This can be 
    particularly useful for cron-triggered data processing jobs.
- Allows testing of serverless MapReduce jobs locally where localstack is used to simulate AWS Lambda 
and other AWS services.
- Allows users to specify different input and output types of storage. Current S3 and DynamoDB are supported, but 
support for other storage can be added easily. 
- Offers abstract infrastructure management.
- Provides a pay-per-execution model.

## Quickstart::Step by Step
ServerlessMR is provided in the form of a Python library. For details on how to download and use this library, check out this
example project: https://github.com/hanglili/Serverless-MapReduce-Test.

## High-level source code walkthrough
ServerlessMR source code resides in ```src/python/serverless_mr```. It contains these different modules 
and Python scripts:
- ```aws_lambda```: Code that defines the class ```LambdaManager```, which deals with anything related to AWS 
Lambda such as the creation of a Lambda.
- ```coordinator```: Code for the lambda handler function of the coordinator.
- ```data_sources```: Code for dealing with different types of input and output storage.
- ```driver```: Code for the driver, where most of the job setup is done such as registering different Lambda
functions for mappers, reducers and coordinator. 
- ```job```: Code for the lambda handler function of the mappers and reducers.
- ```static```: Stores all static variables, configuration files field names, constants and file paths. 
- ```utils```: Code for different utility functions.
- ```web_ui```: Code for the Web Application.
- ```functions```: Objects that store information of the provided map and reduce function pointers.
- ```main.py```: Entry point of a job execution. 

## Contributions
Anyone is more than welcome to contribute to this project. If people contribute to it, then ServerlessMR
can keep advancing. 
