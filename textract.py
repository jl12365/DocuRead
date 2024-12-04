import boto3
import json
import urllib.parse

s3 = boto3.client('s3', region_name='us-east-2')
textract = boto3.client('textract', region_name='us-east-2')


def lambda_handler(event, context):
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        print(f"Processing file from bucket: {source_bucket}, key: {document_key}")

        if not (document_key.lower().endswith('.pdf') or document_key.lower().endswith(('.jpg', '.jpeg', '.png'))):
            print(f"Unsupported file format: {document_key}")
            return {
                'statusCode': 400,
                'body': json.dumps(f"Unsupported file format. Only PDFs, JPGs, and PNGs are supported: {document_key}")
            }

        target_bucket = 'ise391textdata'
        target_key = f'extracted/{document_key.replace(" ", "_")}.json'
        obj_metadata = s3.head_object(Bucket=source_bucket, Key=document_key)

        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': source_bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']
        )

        extracted_text = extract_text_from_textract(response)

        formatted_data = {
            "extracted_text": extracted_text
        }

        extracted_data = json.dumps(formatted_data, indent=2)

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

    except textract.exceptions.InvalidS3ObjectException as invalid_s3_error:
        print(f"Invalid S3 Object: {invalid_s3_error}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Invalid file in S3: {invalid_s3_error}")
        }
    except textract.exceptions.UnsupportedDocumentException as unsupported_doc_error:
        print(f"Unsupported Document: {unsupported_doc_error}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Unsupported document: {unsupported_doc_error}")
        }
    except Exception as e:
        print(f"Error processing file {document_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

def extract_text_from_textract(response):
    blocks = response.get('Blocks', [])
    text = []

    for block in blocks:
        if block['BlockType'] == 'LINE':
            text.append(block['Text'])

    return ' '.join(text)
