FROM public.ecr.aws/lambda/python:3.8
WORKDIR /var/task
COPY src/ .
RUN pip install -r requirements.txt -t .
COPY src/lambda_function.py .
CMD ["lambda_function.lambda_handler"]