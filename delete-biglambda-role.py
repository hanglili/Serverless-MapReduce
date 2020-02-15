import boto3
import botocore

client = boto3.client('iam')

rn='serverless_mr_role'
rp='serverless_mr_policy'

try:
    response = client.delete_role_policy(RoleName=rn, PolicyName=rp)
    print("Success: done deleting role policy")
except botocore.exceptions.ClientError as e:
    print("Error: {0}".format(e))
 
try:
    response = client.delete_role(RoleName=rn)
    print("Success: done deleting role")
except botocore.exceptions.ClientError as e:
    print("Error: {0}".format(e))
