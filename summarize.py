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
        target_bucket = 'ise391finalbucket'
        target_key = f'categorized_data/summary.html'

        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')

        output_text = process_with_openai(text)
        html_output = create_html(output_text)
        save_raw_output(target_bucket, target_key, html_output)

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
            {"role": "user", "content": f"Summarize the transactions and give a high level overview of them:\n{text}"}
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

def create_html(text):
    html_content = "<html><head><title>Summary</title></head><body>"
    html_content += "<h1>Summary of Transactions</h1>"
    
    for line in text.split("\n"):
        html_content += f"<p>{line}</p>"

    html_content += "</body></html>"

    return html_content

def save_raw_output(bucket_name, key, output_text):
    """
    Saves the raw output text to S3 as an HTML file.
    """
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=output_text,
        ContentType='text/html'
    )
