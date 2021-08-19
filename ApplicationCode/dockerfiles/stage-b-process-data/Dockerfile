ARG ECR_COMMON_DATALAKE_REPO_URL
FROM ${ECR_COMMON_DATALAKE_REPO_URL}:latest AS layer
FROM public.ecr.aws/lambda/python:3.8
# Layer Code
WORKDIR /opt
COPY --from=layer /opt/ .

# Function Code
WORKDIR /var/task
COPY src/lambda_function.py .

CMD ["lambda_function.lambda_handler"]