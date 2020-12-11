import json
import os

import boto3
import botocore.exceptions
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging.logger import set_package_logger
from functools import wraps

set_package_logger()

tracer = Tracer()
logger = Logger()

s3_client = boto3.client('s3')
s3_bucket = os.getenv("RESOURCE_BUCKET_NAME") or "lessa-twitch-sample"
s3_key = "airtravel.csv"

# def dummy_decorator(fn):
#     logger.info(f"Decorating {fn}")
#     @wraps(fn)
#     def decorated(*args, **kwargs):
#         logger.info("Calling decorated function...")
#         ret = fn(*args, **kwargs)
#         return ret
#     return decorated

# @tracer.capture_method
# @dummy_decorator
def load_file_from_s3(bucket_name, key):
    try:
        # with tracer.provider.in_subsegment("## load_file_from_s3") as subsegment:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] != "404":
            raise exc
    # Getting the value of the body as csv is fine up to this point, once returned it is no longer available to be read
    return obj

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Replicate https://github.com/awslabs/aws-lambda-powertools-python/issues/238"""
    file_obj = load_file_from_s3(bucket_name=s3_bucket, key=s3_key)

    logger.info({
        "message": "fetched object from S3",
        "s3_response": file_obj
    })

    data = file_obj["Body"].read(file_obj["Content-Length"]).decode('utf-8')
    # data = file_obj["Body"].read().decode('utf-8')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello hello!",
            "data": data
        })
    }
