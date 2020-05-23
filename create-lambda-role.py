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
        "Service": [
            "lambda.amazonaws.com",
            "events.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

rn='serverless_mr_role'
rp='serverless_mr_policy'


try:
    response = client.create_role(RoleName=rn, AssumeRolePolicyDocument=json.dumps(trust_role))
    role_arn = response['Role']['Arn']
    print(role_arn)
    print("Success: done creating role")
    account_id = role_arn.split(":")[4]
    policy_file_path = "policy.json"
    policy = json.loads(open(policy_file_path, 'r').read())
    for statement in policy["Statement"]:
        if "Sid" in statement:
            statement_id = statement["Sid"]
            if statement_id == "passRole":
                statement["Resource"] = "arn:aws:iam::%s:role/*" % account_id
            elif statement_id == "IAMPassRoleForCloudWatchEvents":
                statement["Resource"] = "arn:aws:iam::%s:role/AWS_Events_Invoke_Targets" % account_id

    with open(policy_file_path, "w") as f:
        json.dump(policy, f)
    print("Success: done changing role arn in policy.json")
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


