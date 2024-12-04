import boto3
import json
import urllib.parse
import requests

s3 = boto3.client('s3', region_name='us-east-2')

OPENAI_API_KEY = "..."
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

def lambda_handler(event, context):
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        target_key = f'categorized_data/{document_key.split("/")[-1].replace(".txt", "_output.txt")}'

        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')

        print(f"TEXT: {text}")

        output_text = process_with_openai(text)
        print(f"Processed Output: {output_text}")

        save_raw_output(target_bucket, target_key, output_text)

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


def process_with_openai(text):
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-4",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"Format each transaction into JSON-like format with name, date, amount, and description:\n{text}"}
        ],
        "temperature": 0.7
    }

    try:
        response = requests.post(OPENAI_API_URL, headers=headers, json=data)
        response.raise_for_status()
        response_data = response.json()
        print(f"OpenAI Response: {response_data}")

        return response_data["choices"][0]["message"]["content"]

    except requests.exceptions.RequestException as e:
        print(f"Error invoking OpenAI API: {str(e)}")
        raise


def save_raw_output(bucket_name, key, output_text):
    """
    Saves the raw output text to S3 as a plain text file.
    """
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=output_text,
        ContentType='text/plain'
    )
