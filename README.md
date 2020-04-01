# Serverless-MR
> Serverless MapReduce

[![Build Status](https://travis-ci.org/hanglili/serverless-mr.svg?branch=master)](https://travis-ci.org/hanglili/serverless-mr)

Serverless-MR is a MapReduce framework to be deployed and run on a serverless platform. Currently it supports AWS 
serverless functions (AWS Lambda). Serverless-MR can be seen as a light-weight alternative of Hadoop MapReduce.

Unlike Hadoop MapReduce, Serverless-MR allows users to perform big data processing with no operational effort and 
no prior Hadoop knowledge. Nowadays, Hadoop is one of the most popular big data processing frameworks, however 
it is also undeniable that it has a deep learning curve and requires users to manage the underlying infrastructure. 
By minimising the number of components in the framework and abstracting the infrastructure management, Serverless-MR 
simplifies data processing for users, who now can focus more on actual data problem.

Serverless-MR is also very easy to set up, since many of the manual steps required for the traditional data processing 
frameworks are automated. One such step that is automated is AWS CloudFormation. 

Serverless-MR is particularly-suited to run ad-hoc MapReduce jobs.

## Architecture
Serverless-MR is structured using the following architecture:
![Architecture](images/Serverless-MR-job-timeline.svg?raw=true "")

## Key Features
- Serverless-MR offers 2 modes of running data processing jobs:
    1. Serverful driver: In this mode, the driver runs on the local machine. 
    1. Serverless driver: Completely serverless mode where even the driver is run in AWS Lambda.
- It allows testing serverless MapReduce jobs locally where docker containers are used to simulate AWS Lambdas 
and other AWS services.
- It allows users to specify different input and output data sources of different types of storage (S3, DynamoDB).
- It offers abstract infrastructure management.
- It provides a pay-per-execution model for every job.

## Quickstart::Step by Step
Serverless-MR is provided in the form of a python library. For details on how to use this library, check out this
example project: https://github.com/hanglili/Serverless-MapReduce-Test.

## High-level source code walkthrough
Serverless-MR source code is the module in ```src/python/serverless_mr```. It contains these different submodules 
and scripts:
- ```aws_lambda```: contains code that defines the class LambdaManager, which deals with anything related to AWS Lambda 
such as creation of a Lambda.
- ```coordinator```: contains code for lambda handler function of the reduce coordinators.
- ```data_sources```: contains code for dealing with different types of input and output storage.
- ```driver```: contains code for the driver, where most of the job setup is done such as registering different Lambdas 
for mappers, reducers and reduce coordinators. 
- ```job```: contains code for lambda handler function of the mappers and reducers.
- ```static```: stores all static variables, configuration files field names, constants and file paths. 
- ```utils```: contains code for different utility functions.
- ```main.py```: the entry point of a job execution. 


## Contributions
Anyone is more than welcome to contribute to this project. By making it easily contributable, it can benefit from
from everyone's contributions. Hence this project can keep growing and covers all different types of data processing jobs.