aws2 dynamodb list-tables | jq .'TableNames[]' -r | grep -v serverless-mapreduce-storage-input | xargs -ITABLE -n 1 aws2 dynamodb delete-table --table-name TABLE

aws2 s3 ls | cut -d" " -f 3 | grep -v 'serverless-mr-code\|serverless-mapreduce-storage$' | xargs -I{} aws2 s3 rb s3://{} --force

aws2 logs describe-log-groups --query 'logGroups[*].logGroupName' --output table | \
awk '{print $2}' | grep ^/aws/lambda | while read x; do  echo "deleting $x" ; aws2 logs delete-log-group --log-group-name $x; done