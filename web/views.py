# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  user_role = session['role']
  return render_template('annotate.html', s3_post=presigned_post, user_role=user_role)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  job_id = str(uuid.uuid4())

  # Create a job item and persist it to the annotations database
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  input_file_name = s3_key.split('/')[-1]
  submit_time = int(time.time())  #   Python Docs   https://docs.python.org/3/library/time.html#time.time

  data = {
      'job_id': job_id,
      'user_id': session['primary_identity'],
      'input_file_name':input_file_name,
      's3_inputs_bucket': bucket_name,
      's3_key_input_file': s3_key,
      'submit_time': submit_time,
      'job_status': "PENDING"
  }
  table.put_item(Item=data)   #   Boto3 AWS   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item

  # Publish a notification message to the SNS topic
  sns_client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  
  profile = get_profile(session['primary_identity'])
  data['email'] = profile.email

  response = sns_client.publish(
      TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
      Message=json.dumps({'default': json.dumps(data)}),
      MessageStructure='json'
  ) #  Boto3 AWS   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    user_id = session['primary_identity']
    
    # Retrieve user's annotations from the database
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.query(
        IndexName='user_id-index',
        ExpressionAttributeValues={':user_id': session['primary_identity']},
        KeyConditionExpression='user_id = :user_id'
    )
    
    annotations = []
    for item in response['Items']:
      annotation = {
        'job_id': item['job_id'],
        'input_file_name': item['input_file_name'],
        'submit_time': datetime.fromtimestamp(int(item['submit_time'])).strftime('%Y-%m-%d %H:%M'),
        'job_status': item['job_status']
      }
      annotations.append(annotation)
    
    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    user_id = session['primary_identity']
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.query(
        KeyConditionExpression='job_id = :job_id',
        ExpressionAttributeValues={':job_id': id}
    )

    if not response['Items']:
        return render_template('error.html', 
            title='Not Found', 
            alert_level='warning',
            message='The requested annotation job does not exist.'
        ), 404

    item = response['Items'][0]

    if item['user_id'] != user_id:
        return render_template('error.html', 
            title='Not Authorized', 
            alert_level='danger',
            message='You are not authorized to view this annotation job.'
        ), 403

    result_file_url = ''
    if 's3_key_result_file' in item:
        bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
        result_key_name = item['s3_key_result_file']
        result_file_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': result_key_name},
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
        )

    complete_time_value = item.get('complete_time')
    free_access_expired = False
    if complete_time_value and session.get('role') == 'free_user':
        try:
            complete_time = datetime.fromtimestamp(int(complete_time_value))
            free_access_expired = datetime.now() > (complete_time + timedelta(minutes=5)) # timedelta Python Docs https://docs.python.org/3/library/datetime.html#timedelta
        except ValueError:
            print(f"Error converting complete_time: {complete_time_value}")

    annotation_details = {
        'job_id': id,
        'user_id': user_id,
        'submit_time': datetime.fromtimestamp(int(item['submit_time'])),
        'input_file_name': item['input_file_name'],
        'job_status': item['job_status'],
        'complete_time': datetime.fromtimestamp(int(item['complete_time'])) if 'complete_time' in item else None,
        'result_file_url': result_file_url,
        's3_key_log_file': item.get('s3_key_log_file')
    }

    return render_template('annotation_details.html', annotation=annotation_details, free_access_expired=free_access_expired)

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    user_id = session['primary_identity']
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    response = table.query(
        KeyConditionExpression='job_id = :job_id',
        ExpressionAttributeValues={':job_id': id}
    )

    if not response['Items']:
        return render_template('error.html', 
            title='Not Found', 
            alert_level='warning',
            message='The requested annotation job does not exist.'
        ), 404

    item = response['Items'][0]

    if item['user_id'] != user_id:
        return render_template('error.html', 
            title='Not Authorized', 
            alert_level='danger',
            message='You are not authorized to view the log file for this annotation job.'
        ), 403

    if 's3_key_log_file' not in item:
        return render_template('error.html', 
            title='File Not Available', 
            alert_level='warning',
            message='The log file for this annotation job is not available.'
        ), 404

    log_response = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=item['s3_key_log_file'])
    log_file_contents = log_response['Body'].read().decode('utf-8')

    return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)

"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"
    print(f"User {session['primary_identity']} subscribed to premium features.")
    data = {"user_id": session['primary_identity']}
    sns_client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    response = sns_client.publish(
      TopicArn=app.config['RESTORE_ARN'],
      Message=json.dumps({'default': json.dumps(data)}),
      MessageStructure='json'
    )

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
