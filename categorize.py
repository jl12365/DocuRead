import boto3
import json
import urllib.parse
import os


s3 = boto3.client('s3', region_name='us-east-2')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
MAX_TOKENS = 8000

def lambda_handler(event, context):
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        target_bucket = 'ise391report'
        
        file_response = s3.get_object(Bucket=source_bucket, Key=document_key)
        text = file_response['Body'].read().decode('utf-8')
        
        print(text)

        prompts = create_prompts(text)
        
        all_categorized_data = []
        for prompt in prompts:
            categorized_data = analyze_with_titan(prompt)
            all_categorized_data.extend(categorized_data)
        
        # Save results to the S3 bucket
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
    prompts = [' '.join(chunk) for chunk in chunks]
    return prompts

def analyze_with_titan(prompt):
    formatted_prompt = f"Process the text and output the data in JSON format:\n{prompt}"   

    try:
        response = bedrock.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=json.dumps({
                "inputText": formatted_prompt,
            }),
            contentType="application/json",
            accept="application/json"
        )

        response_body = json.loads(response['body'].read().decode('utf-8'))
        generated_text = response_body.get('generatedText', 'No output generated')
        return [{"summary": generated_text}]
    except Exception as e:
        print(f"Error invoking Amazon Titan model: {str(e)}")
        raise

def save_categorized_data(bucket_name, key, data):
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
