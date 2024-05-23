# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
import uuid
from botocore import exceptions
from configparser import ConfigParser

# Read the configuration file
config = ConfigParser()
config.read('restore_config.ini')

# Get the configuration values
table_name = config['aws']['annotations_table']
result_bucket = config['aws']['AWS_S3_RESULTS_BUCKET']
glacier_vault = config['aws']['AWS_GLACIER_VAULT']
queue_url = config['aws']['SQS_RESTORE_URL']
sns_topic_arn = config['aws']['SNS_THAW_TOPIC']

# Create boto3 clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
glacier = boto3.client('glacier')
sns = boto3.client('sns')

def process_message(message):
    # Parse the JSON message
    message_body = json.loads(message['Body'])
    user_id = message_body['user_id']

    # Get the user's archived jobs
    archived_jobs = get_archived_jobs(user_id)

    # Initiate restore for each archived job
    for job_id in archived_jobs:
        initiate_restore(job_id)

def get_archived_jobs(user_id):
    # Query DynamoDB to get the user's archived jobs
    table = dynamodb.Table(table_name)
    response = table.query(
        IndexName='user_id_index',
        KeyConditionExpression='user_id = :user_id',
        FilterExpression='attribute_exists(results_file_archive_id)',
        ExpressionAttributeValues={':user_id': user_id}
    )
    return [item['job_id'] for item in response['Items']]

def initiate_restore(job_id):
    # Get the archive ID from DynamoDB
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'job_id': job_id})
    item = response['Item']
    archive_id = item['results_file_archive_id']

    # Initiate the restore job in Glacier
    try:
        glacier_response = glacier.initiate_job(
            vaultName=glacier_vault,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited'
            }
        )# glacier job parameters https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
        job_id = glacier_response['jobId']
        print(f"Restore job initiated for archive ID: {archive_id}")

        # Publish a message to SNS
        data = {
            'jobId': job_id,
            'job_id': item['job_id'],
            'user_id': item['user_id'],
            'input_file_name': item['input_file_name'],
            'job_status': item['job_status'],
            's3_key_result_file': item['s3_key_result_file'],
            'results_file_archive_id': archive_id
        }
        sns_response = sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(data)
        )
        print(f"SNS message published for restore job: {job_id}")

    except exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'PolicyEnforcedException':
            # Retry with Standard retrieval option
            glacier_response = glacier.initiate_job(
                vaultName=glacier_vault,
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Tier': 'Standard'
                } # glacier job parameters https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
            )
            job_id = glacier_response['jobId']
            print(f"Restore job initiated with Standard tier for archive ID: {archive_id}")

            # Publish a message to SNS
            data = {
                'jobId': job_id,
                'job_id': item['job_id'],
                'user_id': item['user_id'],
                'input_file_name': item['input_file_name'],
                'job_status': item['job_status'],
                's3_key_result_file': item['s3_key_result_file'],
                'results_file_archive_id': archive_id
            }
            sns_response = sns.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(data)
            )
            print(f"SNS message published for restore job: {job_id}")

        else:
            print(f"Error initiating restore job: {e}")
            raise

def main():
    while True:
        # Receive SQS messages
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        ) # receive_message https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message

        if 'Messages' in response:
            message = response['Messages'][0]
            process_message(message)

            # Delete the processed message
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle) # delete_message https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        else:
            print("No messages in the queue. Waiting...")

if __name__ == '__main__':
    main()
### EOF