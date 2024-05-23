# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import boto3
import logging
from configparser import SafeConfigParser

# Read the configuration file
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Create boto3 clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')
dynamodb = boto3.resource('dynamodb')

# Get the DynamoDB table
table = dynamodb.Table(config['aws']['annotations_table'])

while True:
    try:
        # Receive messages from the thaw queue
        response = sqs.receive_message(
            QueueUrl=config['aws']['SQS_THRAW_URL'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            message_body = message['Body']
            json_body = json.loads(message_body)
            data = json.loads(json_body['Message'].replace("'", "\""))
            
            result_file_key = data['s3_key_result_file']
            archive_id = data['results_file_archive_id']
            job_id = data['jobId']
        else:
            continue

        # Check if the Glacier job is completed
        job_description = glacier.describe_job(
            vaultName=config['aws']['AWS_GLACIER_VAULT'],
            jobId=job_id
        ) # describe_job()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.describe_job

        if job_description['Completed']:
            # Retrieve the restored file from Glacier
            job_output = glacier.get_job_output(
                vaultName=config['aws']['AWS_GLACIER_VAULT'],
                jobId=job_id
            ) # get_job_output()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output

            # Upload the restored file to S3
            s3.put_object(
                Body=job_output['body'].read(),
                Bucket=config['aws']['AWS_S3_RESULTS_BUCKET'],
                Key=result_file_key
            ) # put_object()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object

            # Delete the message from the thaw queue
            sqs.delete_message(
                QueueUrl=config['aws']['SQS_THRAW_URL'],
                ReceiptHandle=receipt_handle
            )

            # Delete the archive from Glacier
            glacier.delete_archive(
                vaultName=config['aws']['AWS_GLACIER_VAULT'],
                archiveId=archive_id
            )# delete_archive()  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive

    except glacier.exceptions.NoSuchJobExecutionException as e:
        # Log and handle cases where the specified Glacier job does not exist
        logging.error(f"No such job found: {e}")

    except Exception as e:
        # Log any other exceptions that occur during processing
        logging.error(f"Error processing message: {e}")

        # Delete the SQS message to avoid reprocessing in case of errors
        if 'receipt_handle' in locals():
            sqs.delete_message(
                QueueUrl=config['aws']['SQS_THRAW_URL'],
                ReceiptHandle=receipt_handle
            )
### EOF