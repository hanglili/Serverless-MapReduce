#!/usr/bin/env bash

s3_bucket_name=$1
MY_ACCOUNT_ID=$2

#s3_bucket_path='s3://'$s3_bucket_name
#echo "$s3_bucket_path"

#aws2 s3 mb "$s3_bucket_path"
#sed -i "" "s/s3:::YOUR-BUCKET-NAME-HERE/s3:::$s3_bucket_name/" policy.json

python create-biglambda-role.py
export serverless_mapreduce_role=arn:aws:iam::$MY_ACCOUNT_ID:role/serverless_mr_role
#sed -i "" "s/YOUR-BUCKET-NAME-HERE/$s3_bucket_name/" src/python/configuration/static-job-info.json
#cat src/python/configuration/static-job-info.json

#./xray_mac -o -n us-east-1 &

# shellcheck disable=SC2164