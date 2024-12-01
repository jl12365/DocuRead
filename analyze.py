import boto3
import json
from urllib.parse import unquote

s3 = boto3.client('s3', region_name='us-east-2')

def lambda_handler(event, context):
    # Get bucket and key from event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    document_key = unquote(event['Records'][0]['s3']['object']['key'])
    
    try:
        # Load JSON from S3
        response = s3.get_object(Bucket=source_bucket, Key=document_key)
        content = response['Body'].read().decode('utf-8')
        textract_data = json.loads(content)
        
        # Extract relevant text
        text_lines = extract_text_lines(textract_data)
        
        # save data to another s3 bucket 'ise391report'
        target_bucket = 'ise391report'
        target_key = f'extracted_text/{document_key}.txt'
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body="\n".join(text_lines),
            ContentType='text/plain'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Extracted text saved to s3://{target_bucket}/{target_key}")
        }
    
    except Exception as e:
        print(f"Error processing file {document_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

# helper to analyze each line in the extracted Textract document
def extract_text_lines(data):
    text_lines = []
    for block in data.get('Blocks', []):
        if block.get('BlockType') == 'LINE':
            text_lines.append(block.get('Text', ''))
    return text_lines
