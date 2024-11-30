import boto3
import json

# Initialize AWS clients
s3 = boto3.client('s3')
textract = boto3.client('textract')

def lambda_handler(event, context):
    # 1. Extract bucket name and object key from the S3 event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    document_key = event['Records'][0]['s3']['object']['key']

    # Define target bucket for extracted data
    target_bucket = 'ise391lastbucket'
    target_key = f'extracted/{document_key}.json'

    try:
        # 2. Call Textract to analyze the document
        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': source_bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']  # Extract tables and form data
        )
        
        # 3. Process the Textract response if needed
        extracted_data = json.dumps(response, indent=2)  # Convert response to JSON
        
        # 4. Upload the extracted data to the target S3 bucket
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=extracted_data,
            ContentType='application/json'
        )
        
        print(f"Extracted data saved to s3://{target_bucket}/{target_key}")
        return {
            'statusCode': 200,
            'body': json.dumps('Success')
        }
    except Exception as e:
        print(f"Error processing file {document_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
