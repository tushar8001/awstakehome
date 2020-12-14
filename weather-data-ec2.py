import boto3
import json
import xarray as xr
import time
import os
import csv
import shutil

sqs = boto3.resource('sqs', 'us-east-2')
s3 = boto3.resource('s3')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='weather-data')

def lambda_handler(event, context):

    body = json.loads(event.body)
    # Delete the tmp directory
    if os.path.isdir('/tmp/weather'):
        shutil.rmtree('/tmp/weather')
    # Create the tmp directory
    os.mkdir('/tmp/weather')
    
    # Load the message from the SQS message
    body_message = json.loads(body['Message'])
    metric = body_message['name']
    
    # Get the s3 bucket, key for the cdf file
    s3_bucket = body_message['bucket']
    s3_key = body_message['key']
    output_file = '/tmp/weather/' + s3_key
    # Download the cdf file
    s3.Bucket(s3_bucket).download_file(s3_key, output_file)

    # Open the dataset from cdf file using xarray
    ds = xr.open_dataset(output_file)
    time = str(ds.get('time').data)

    # Convert dataset to dataframe
    df = ds.to_dataframe()

    # Keep the metric 
    series = df[metric]
    series = series.reset_index()

    # Output CSV file name
    csv_file = '/tmp/weather/{}-{}.csv'.format(metric, time)
    series['time'] = time
    
    # Write to csv file locally
    series.to_csv(csv_file)
    csv_columns = list(series.columns)

    # Upload csv file to s3 bucket
    output_bucket = 'tushar-bigdata-takehome-curated'
    output_key = '{}/{}.csv'.format(metric, time)
    s3.Bucket(output_bucket).upload_file(csv_file, output_key)

    # Upload headers file to s3 bucket
    output_key = '{}-headers.csv'.format(metric)
    f = open('/tmp/headers.csv', 'w')
    f.write(str(csv_columns))
    f.close()
    s3.Bucket(output_bucket).upload_file('/tmp/headers.csv', output_key)
    print("done")


while True:
    # Read and process messages
    for message in queue.receive_messages(MaxNumberOfMessages=10):
        print(message)
        try:
            lambda_handler(message, None)
        except:
            pass
        message.delete()
