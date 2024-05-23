# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
sys.path.append(os.path.abspath('..'))
import boto3
import json
from botocore import exceptions
from datetime import datetime, timedelta
from configparser import SafeConfigParser
import helpers

# Read the configuration file
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Get the configuration values
table_name = config['aws']['annotations_table']
result_bucket = config['aws']['AWS_S3_RESULTS_BUCKET']
glacier_vault = config['aws']['AWS_GLACIER_VAULT']
queue_url = config['aws']['SQS_ARCHIVE_URL']

# Create boto3 clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
glacier = boto3.client('glacier')

def process_message(message):
    # Parse the JSON message
    message_body = json.loads(message['Body'])
    job_data = json.loads(message_body['Message'])

    user_id = job_data['user_id']
    job_id = job_data['job_id']

    # Check the user role
    roles = helpers.get_user_role(id=user_id) # I checked helpers.py and psycopg2 module  https://www.psycopg.org/docs/usage.html
    user_role = roles[4]
    if user_role == 'premium_user':
        print(f"User {user_id} is a premium user. Skipping archival.")
        return

    # Get the job information
    job_item = get_job_item(job_id)
    result_key = job_item['s3_key_result_file']

    # Archive the result file to Glacier
    archive_id = archive_result_file(result_bucket, result_key, glacier_vault)

    # Update the DynamoDB record
    update_job_item(job_id, archive_id)

def get_job_item(job_id):
    # Get the job information from DynamoDB
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'job_id': job_id})
    return response['Item']

def archive_result_file(bucket, key, vault):
    # Get the result file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read()

    # Upload to Glacier
    try:
        glacier_response = glacier.upload_archive(vaultName=vault, body=file_content) # upload_archive()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
        archive_id = glacier_response['archiveId']
        print(f"Archive ID: {archive_id}")

        # Delete the S3 object
        s3.delete_object(Bucket=bucket, Key=key) # delete_object()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        return archive_id
    except exceptions.ClientError as e:
        print(f"Error uploading to Glacier: {e}")
        raise

def update_job_item(job_id, archive_id):
    # Update the DynamoDB record
    table = dynamodb.Table(table_name)
    try:
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET results_file_archive_id = :archive_id',
            ExpressionAttributeValues={':archive_id': archive_id}
        )
        print(f"DynamoDB update response: {response}")
    except exceptions.ClientError as e:
        print(f"Error updating DynamoDB: {e}")
        raise

def main():
    while True:
        # Receive SQS messages
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            process_message(message)

            # Delete the processed message
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        else:
            print("No messages in the queue. Waiting...")

if __name__ == '__main__':
    main()