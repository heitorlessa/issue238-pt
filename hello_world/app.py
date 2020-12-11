import copy
import io
import json
import os
from pprint import pprint

import boto3
import botocore.exceptions
import pandas as pd
from aws_lambda_powertools import Logger, Tracer

tracer = Tracer()
logger = Logger()

s3_client = boto3.client('s3')
s3_bucket = os.getenv("RESOURCE_BUCKET_NAME") or "lessa-twitch-sample"
s3_key = "airtravel.csv"

@tracer.capture_method
def load_file_from_s3(bucket_name, key):
    try:
        logger.info("Fetching S3 object...")
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] != "404":
            raise exc
    # Getting the value of the body as csv is fine up to this point, once returned it is no longer available to be read
    return obj

@tracer.capture_method
def s3_shape_obj_to_pandas(obj):
    """Reads a csv file containing the shape data and returns this as a pandas dataframe
    
    UPDATE: Removed columns as I'm using a random CSV file
    """
    # streamed_object = obj["Body"]
    # csv_file = io.BytesIO(streamed_object.read())
    csv_file = io.BytesIO(obj["Body"].read())
    csv_file_copy = copy.deepcopy(csv_file)
    
    logger.info({
        "csv_content_copy": csv_file_copy.read()
    })

    # Read the shape file using Pandas
    # Exception is triggered here because the retrieved data is equal to b''
    df = pd.read_csv(csv_file, index_col=0, skiprows=2)

    return df

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Replicate https://github.com/awslabs/aws-lambda-powertools-python/issues/238"""
    file_obj = load_file_from_s3(bucket_name=s3_bucket, key=s3_key)
    shape = s3_shape_obj_to_pandas(file_obj)

    pprint(shape)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello hello!",
            "data": shape.to_json()
        })
    }
