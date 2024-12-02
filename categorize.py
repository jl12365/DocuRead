import boto3
import json
import urllib.parse
import requests
import os


s3 = boto3.client('s3', region_name='us-east-2')


hugging_key = os.getenv('hugging_face')

MAX_TOKENS = 8192 

def lambda_handler(event, context):
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        
        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')
        
        prompts = create_prompts(text)
        
        all_categorized_data = []
        for prompt in prompts:
            categorized_data = analyze_with_hugging_face(prompt)
            all_categorized_data.extend(categorized_data)
        
        target_key = f'categorized_data/{document_key.split("/")[-1].replace(".txt", ".json")}'
        
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


def create_prompts(text):
    tokens = text.split() 
    chunks = [tokens[i:i+MAX_TOKENS] for i in range(0, len(tokens), MAX_TOKENS)]  
    prompts = []

    for chunk in chunks:
        chunk_text = ' '.join(chunk) 
        prompts.append(chunk_text)

    return prompts


def analyze_with_hugging_face(prompt):
    try:
        url = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
        
        headers = {
            "Authorization": f"Bearer {hugging_key}"
        }
        
        data = {
            "inputs": prompt
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            summary = response.json()[0]['summary_text']
            return [{"summary": summary}]
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return []
    except Exception as e:
        print(f"Error invoking Hugging Face API: {str(e)}")
        raise
    
def save_categorized_data(bucket_name, key, data):
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
