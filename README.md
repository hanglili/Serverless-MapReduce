# Serverless-MR
> Serverless MapReduce

[![Build Status](https://travis-ci.org/hanglili/serverless-mr.svg?branch=master)](https://travis-ci.org/hanglili/serverless-mr)

Serverless-MR is a MapReduce framework to be deployed and run on a serverless platform. Currently it supports AWS 
serverless functions (AWS Lambda). Serverless-MR can be seen as a light-weight alternative to Hadoop MapReduce.

Unlike Hadoop MapReduce, Serverless-MR allows users to perform big data processing with no operational effort and 
no prior Hadoop knowledge. Nowadays, Hadoop is one of the most popular big data processing frameworks, however 
it is undeniable that it has a deep learning curve and requires users to manage the underlying infrastructure. 
By minimising the number of components in the framework and abstracting the infrastructure management, Serverless-MR 
simplifies data processing for users, who now can focus more on actual data problems.

Due to the serverless nature of the framework, users only get charged for the time that a job is being executed, 
getting rid of the superfluous cost caused by idle periods of the underlying infrastructure used to execute the job. 
Hence, Serverless-MR is particularly-suited to run ad-hoc MapReduce jobs.

Serverless-MR is also very easy to set up, since many of the manual steps required for the traditional data processing 
frameworks have been automated. One such automated step is AWS services deployments with AWS CloudFormation. 

## Architecture
Serverless-MR has been designed with the following architecture:
![Architecture](images/Serverless-MR-job-timeline.png?raw=true "")

## Key Features
- It offers 2 modes of running any data processing jobs:
    1. Serverful driver: In this mode, the driver runs on the local machine. 
    1. Serverless driver: Completely serverless mode where even the driver is run in AWS Lambda. This can be 
    particularly useful for cron-triggered data processing jobs.
- It allows testing serverless MapReduce jobs locally where docker containers are used to simulate AWS Lambda 
and other AWS services.
- It allows users to specify different input and output data sources of different types of storage (S3, DynamoDB).
- It offers abstract infrastructure management.
- It provides a pay-per-execution model for every job.

## Quickstart::Step by Step
Serverless-MR is provided in the form of a Python library. For details on how to use this library, check out this
example project: https://github.com/hanglili/Serverless-MapReduce-Test.

## High-level source code walkthrough
Serverless-MR source code is the module in ```src/python/serverless_mr```. It contains these different submodules 
and Python scripts:
- ```aws_lambda```: contains code that defines the class ```LambdaManager```, which deals with anything related to AWS 
Lambda such as the creation of a Lambda.
- ```coordinator```: contains code for the lambda handler function of the reduce coordinators.
- ```data_sources```: contains code for dealing with different types of input and output storage.
- ```driver```: contains code for the driver, where most of the job setup is done such as registering different Lambdas 
for mappers, reducers and reduce coordinators. 
- ```job```: contains code for the lambda handler function of the mappers and reducers.
- ```static```: stores all static variables, configuration files field names, constants and file paths. 
- ```utils```: contains code for different utility functions.
- ```main.py```: the entry point of a job execution. 


## Contributions
Anyone is more than welcome to contribute to this project. If this project can benefit from everyone's contributions, 
then it can keep expanding and covers all different types of data processing jobs using a serverless architecture.
