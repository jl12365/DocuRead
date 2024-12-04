import boto3
import json
import urllib.parse
import os
from datetime import datetime

s3 = boto3.client('s3', region_name='us-east-2')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

MAX_TOKENS = 1000

def lambda_handler(event, context):
    try:
        #info from event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        #info to store data into bucke
        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')

        if text.strip():
            all_categorized_data = analyze_with_titan(text)
            target_key = f'categorized_data/output_{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
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
    # formatted_prompt = f"Put the following transactions into JSON format with fields of date, payee, amount, and description:\n{prompt}"
    formatted_prompt = "why are cars expensive?"

    native_request = {
        "inputText" : formatted_prompt,
        "textGenerationConfig":{
            "maxTokenCount": 1000,
            "temperature": 0.5,
            "topP": 0.9
        }
    }

    request = json.dumps(native_request)

    try:
        response = bedrock.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=json.dumps({
                "inputText": request,
            }),
            contentType="application/json",
            accept="application/json"
        )

        response_body = json.loads(response["body"].read())
        generated_text = response_body["results"][0]["outputText"]
        return [{"summary": generated_text}]

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
