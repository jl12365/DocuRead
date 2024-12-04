import boto3
import json
import urllib.parse
import os

s3 = boto3.client('s3', region_name='us-east-2')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        target_key = f'categorized_data/{document_key.split("/")[-1].replace(".txt", ".json")}'
        
        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')
    
        print(f"TEXT: {text}")
        all_categorized_data = analyze_with_titan(text)
        print(f"DATA: {all_categorized_data}")
        save_categorized_data(target_bucket, target_key, all_categorized_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Categorized data saved to s3://{target_bucket}/{target_key}")
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

def analyze_with_titan(prompt):
    formatted_prompt = f"Format each transaction into JSON foramt with name, date, amount, and description:\n{prompt}"   

    try:
        response = bedrock.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=json.dumps({
                "inputText": formatted_prompt,
            }),
            contentType="application/json",
            accept="application/json"
        )

        response_body = response['body'].read().decode('utf-8')
        response_data = json.loads(response_body)
        # generated_text = response_data.get('results', [{}])[0].get('outputText', 'No output generated')
        return (response_data)

    except Exception as e:
        print(f"Error invoking Amazon Titan model: {str(e)}")
        raise

def save_categorized_data(bucket_name, key, data):
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )

       print(f"Data saved to s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error saving data to S3: {str(e)}")
        raise
