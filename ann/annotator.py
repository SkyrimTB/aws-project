from flask import Flask, request, jsonify, render_template
import boto3
import subprocess
import json
import os
import time
from botocore.exceptions import ClientError
from configparser import SafeConfigParser # Python ConfigParser   https://docs.python.org/3/library/configparser.html

app = Flask(__name__)

config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['secret_access_key']
REGION = config['aws']['region']
SQS_QUEUE_URL = config['sqs']['queue_url']

def process_message(message):
    # Extract job parameters from the message body
    job_id = message['job_id']
    input_file_name = message['input_file_name']
    bucket_name = message['s3_inputs_bucket']
    s3_key = message['s3_key_input_file']
    email = message['email']
    user_id = message['user_id']

    # Get the input file S3 object and copy it to a local file
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION)
    local_dir = f'/home/ec2-user/mpcs-cc/anntools/data/{job_id}'
    os.makedirs(local_dir, exist_ok=True)
    local_file_path = f'{local_dir}/{input_file_name}'
    s3.download_file(bucket_name, s3_key, local_file_path)

    # Launch the annotation process
    try:
        subprocess.Popen(['python', 'run.py', local_file_path, job_id, email, user_id])
        print(f"Annotation process launched for job {job_id}")
    except Exception as e:
        print(f"Error launching annotation process for job {job_id}: {str(e)}")

if __name__ == '__main__':
    # Connect to SQS and get the message queue
    sqs = boto3.client('sqs', region_name=REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Poll the message queue in a loop
    while True:
        try:
            # Receive messages from the queue with long polling
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageAttributeNames=['All'],
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20  # Long polling interval
            ) #  Python Boto3   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html

            # Check if a message was received
            if 'Messages' in response:
                for message in response['Messages']:
                    message = response['Messages'][0]
                    # Extract the message body
                    message_body = json.loads(message['Body'])
                    # Extract the job details from the message body
                    job_details = json.loads(message_body['Message'])

                    # Process the message
                    process_message(job_details)

                    # Delete the message from the queue
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print("Message deleted from the queue")
            else:
                print("No messages in the queue. Polling again...")
        except ClientError as e:
            print(f'Error: {e.response["Error"]["Message"]}')

        # Wait for a short interval before polling again
        time.sleep(1)