#!/usr/bin/sh

echo "Creating S3 bucket: csv-uploads"
awslocal s3 mb s3://csv-uploads || echo "Bucket csv-uploads may already exist"

