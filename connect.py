import boto3
import os

s3 = boto3.client('s3')

bucket_name = 'ise391test'
#change file path to yoru own document you want to upload
file_path = r'C:\Users\jason\projects\DocuRead\Documents\amazon filing 2.pdf'
s3_key = 'amazon_filing_2.pdf'

try:
    s3.upload_file(file_path, bucket_name, s3_key)
    print("file was successfully uploaded to s3")
except Exception as e:
    print("an error occured: {e}")





