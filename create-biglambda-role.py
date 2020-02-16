import boto3
import json

client = boto3.client('iam')

trust_role = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

rn='serverless_mr_role'
rp='serverless_mr_policy'

try:
    response = client.create_role(RoleName=rn, AssumeRolePolicyDocument=json.dumps(trust_role))
    print(response['Role']['Arn'])
    print("Success: done creating role")
except Exception as e:
    print("Error: {0}".format(e))

try:
    with open('policy.json') as json_data:
        response = client.put_role_policy(RoleName=rn, PolicyName=rp,
            PolicyDocument=json.dumps(json.load(json_data))
        )
        print("Success: done adding inline policy to role")
except Exception as e:
    print("Error: {0}".format(e))
