import boto3
import json

s3 = boto3.client('s3')
textract = boto3.client('textract')

def lambda_handler(event, context):
    
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    document_key = event['Records'][0]['s3']['object']['key']

    target_bucket = 'ise391lastbucket'
    target_key = f'extracted/{document_key}.json'

    try:
        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': source_bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS'] 
        )
        
        extracted_data = json.dumps(response, indent=2) 
        
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
