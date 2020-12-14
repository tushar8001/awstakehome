from __future__ import print_function
import json
import boto3
import os
import csv
import xarray as xr
import shutil

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        # Delete the tmp directory
        if os.path.isdir('/tmp/weather'):
            shutil.rmtree('/tmp/weather')
        
        # Create the tmp directory
        os.mkdir('/tmp/weather')
        # Load the message from the SQS message
        body=json.loads(record["body"])
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
        new_df = series.reset_index()
    
        json_data_lists = new_df.to_json(orient='records', lines=True).splitlines()[:1000]
        json_data = []
        
        # Convert dataframe to list of json dictionaries
        for idx, val in enumerate(json_data_lists):
            if idx % 1000 == 0:
                print(idx)
            val_dict = json.loads(val)
            val_dict['time'] = time
            json_data.append(val_dict)

        if not json_data:
            return

        csv_columns = list(json_data[0].keys())
        csv_columns.sort()
        print(csv_columns)
        
        # Output CSV file name
        csv_file = '/tmp/weather/{}-{}.csv'.format(metric, time)
        
        # Write CSV file
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            for data in json_data:
                writer.writerow(data)

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
