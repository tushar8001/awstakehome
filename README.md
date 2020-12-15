# awstakehome

Weather forecast Files are loaded in public S3 bucket daily every 15-20 mins. Location of the bucket is aws s3 ls s3://aws-earth-mo-atmospheric-mogreps-uk-prd/ --no-sign-request. I need to process the file and see visualize in Quicksight. 

To do that, I am following below high level steps:
1.	I need to create lambda function to convert this file in csv or json
2.	I need to create SQS that listens to SNS to trigger the lambda
3.	Lambda copies file in my private S3 folder s3://tushar-bigdata-lambda-csv
4.	Create Glue job that read metadata for these files and create catalog
5.	Hook up athena on that s3 folder s3://tushar-bigdata-lambda-csv
6.	Run Quicksight instegrating with athena
