import sys
import time
import driver
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['secret_access_key']
S3_INPUTS_BUCKET = config['s3']['inputs_bucket']
S3_RESULTS_BUCKET = config['s3']['results_bucket']
REGION = config['aws']['region']
ANNOTATIONS_TABLE = config['dynamodb']['annotations_table']
PREFIX = config['other']['prefix']
topic_arn = config['sns']['topic_arn']

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_to_s3(file_path, bucket, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION)
    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"File {file_path} uploaded to {bucket}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file: {str(e)}")

def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"Local file {file_path} deleted")
    except Exception as e:
        print(f"Error deleting local file: {str(e)}")

def update_job_status(job_id, status, s3_result_bucket, s3_result_key, s3_log_key):
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(ANNOTATIONS_TABLE)

    update_expression = 'SET job_status = :status'
    expression_attribute_values = {':status': status}

    if status == 'COMPLETED':
        update_expression += ', complete_time = :complete_time, s3_results_bucket = :s3_result_bucket, s3_key_result_file = :s3_result_key, s3_key_log_file = :s3_log_key'
        expression_attribute_values[':complete_time'] = int(time.time())
        expression_attribute_values[':s3_result_bucket'] = s3_result_bucket
        expression_attribute_values[':s3_result_key'] = s3_result_key
        expression_attribute_values[':s3_log_key'] = s3_log_key

    try:
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        print(f"Job {job_id} status updated to {status}")
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException: # Job might be in a different state
        print(f"Job {job_id} status update failed. The job might be in a different state.")

def publish_job_completion(job_id,message):
    sns = boto3.client('sns',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION)
    sns.publish(TopicArn=topic_arn, Message=message)

if __name__ == '__main__':
    if len(sys.argv) > 4:
        input_file_path = sys.argv[1]
        job_id = sys.argv[2]
        email = sys.argv[3]
        user_id = sys.argv[4]

        with Timer():
            driver.run(input_file_path, 'vcf')

        input_file_name = os.path.basename(input_file_path)
        output_file_name = input_file_name.replace('.vcf', '.annot.vcf')
        output_file_path = os.path.join(os.path.dirname(input_file_path), output_file_name)
        log_file_name = input_file_name + '.count.log'
        log_file_path = os.path.join(os.path.dirname(input_file_path), log_file_name)

        prefix = PREFIX
        output_s3_key = f"{prefix}results/{output_file_name}"
        log_s3_key = f"{prefix}logs/{log_file_name}"

        if os.path.exists(output_file_path):
            upload_to_s3(output_file_path, S3_RESULTS_BUCKET, output_s3_key)
            delete_local_file(output_file_path)

        if os.path.exists(log_file_path):
            upload_to_s3(log_file_path, S3_RESULTS_BUCKET, log_s3_key)
            delete_local_file(log_file_path)

        update_job_status(job_id, 'COMPLETED', S3_RESULTS_BUCKET, output_s3_key, log_s3_key)
        data = {
            "email": email,
            "job_id": job_id,
            "user_id": user_id
        }
        publish_job_completion(job_id,str(data))  # Publish notification to SNS topic

    else:
        print("Please provide a valid .vcf file path and job ID as input to this program.")