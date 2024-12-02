import boto3
import json
import urllib.parse
import requests

# Initialize AWS clients
s3 = boto3.client('s3', region_name='us-east-2')

# Set Hugging Face API Key (store it securely, like using AWS Secrets Manager)
HUGGING_FACE_API_KEY = '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'

MAX_TOKENS = 8192  # Maximum token limit for the model

def lambda_handler(event, context):
    try:
        # Get the S3 bucket and file from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        
        # Get file from S3
        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')
        
        # Generate prompt for Hugging Face API (text is now chunked)
        prompts = create_prompts(text)
        
        # Analyze data with Hugging Face API and collect results
        all_categorized_data = []
        for prompt in prompts:
            categorized_data = analyze_with_hugging_face(prompt)
            all_categorized_data.extend(categorized_data)
        
        # Save categorized data to S3
        target_key = f'categorized_data/{document_key.replace(".txt", ".json")}'
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

# Function to create the prompts for Hugging Face API by chunking the input text
def create_prompts(text):
    tokens = text.split()  # Split text into tokens (words)
    chunks = [tokens[i:i+MAX_TOKENS] for i in range(0, len(tokens), MAX_TOKENS)]  # Chunk the tokens
    prompts = []

    for chunk in chunks:
        chunk_text = ' '.join(chunk)  # Join the chunk back into a string
        prompts.append(chunk_text)

    return prompts

# Function to invoke Hugging Face API for summarization
def analyze_with_hugging_face(prompt):
    try:
        # Hugging Face API URL and model endpoint
        url = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
        
        headers = {
            "Authorization": f"Bearer {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}"
        }
        
        data = {
            "inputs": prompt
        }

        # Send the request to Hugging Face API
        response = requests.post(url, headers=headers, json=data)

        # Check if the response is successful
        if response.status_code == 200:
            summary = response.json()[0]['summary_text']
            return [{"summary": summary}]
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return []
    except Exception as e:
        print(f"Error invoking Hugging Face API: {str(e)}")
        raise

# Function to save categorized data back to S3
def save_categorized_data(bucket_name, key, data):
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
