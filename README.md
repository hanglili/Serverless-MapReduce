To run localstack:
```
 TMPDIR=/private$TMPDIR SERVICES=serverless LAMBDA_EXECUTOR=docker LAMBDA_REMOVE_CONTAINERS=false DOCKER_HOST=unix:///var/run/docker.sock  DEBUG=1 localstack start --docker
```

Set environment variables to:
```
serverless_mapreduce_role=arn:aws:iam::123456789:role/serverless_mr_role
```