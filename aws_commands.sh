aws2 dynamodb list-tables | jq .'TableNames[]' -r | grep -v serverless-mapreduce-storage-input | xargs -ITABLE -n 1 aws2 dynamodb delete-table --table-name TABLE

# Does not work
aws2 s3 ls | grep -v serverless-mapreduce-storage | awk '{printf "aws2 s3 rb s3://%s --force\n",$3}'

aws2 logs describe-log-groups --query 'logGroups[*].logGroupName' --output table | \
awk '{print $2}' | grep ^/aws/lambda | while read x; do  echo "deleting $x" ; aws2 logs delete-log-group --log-group-name $x; done