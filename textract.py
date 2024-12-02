import boto3
import json
import urllib.parse

# Initialize AWS clients
s3 = boto3.client('s3', region_name='us-east-2')
textract = boto3.client('textract', region_name='us-east-2')


def lambda_handler(event, context):
    try:
        # 1. Extract bucket and file details from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        document_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        print(f"Processing file from bucket: {source_bucket}, key: {document_key}")

        # Validate that the file is a supported format (PDF, JPG, PNG)
        if not (document_key.lower().endswith('.pdf') or document_key.lower().endswith(('.jpg', '.jpeg', '.png'))):
            print(f"Unsupported file format: {document_key}")
            return {
                'statusCode': 400,
                'body': json.dumps(f"Unsupported file format. Only PDFs, JPGs, and PNGs are supported: {document_key}")
            }

        # 2. Define the target bucket and key for extracted data
        target_bucket = 'ise391lastbucket'
        target_key = f'extracted/{document_key.replace(" ", "_")}.json'

        # 3. Check if the file is valid by fetching metadata
        obj_metadata = s3.head_object(Bucket=source_bucket, Key=document_key)
        print(f"Retrieved metadata for {document_key}: {obj_metadata}")

        # 4. Call Textract to analyze the document or image
        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': source_bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']  # Extract tables and form data
        )

        # 5. Process the Textract response
        extracted_data = json.dumps(response, indent=2)  # Convert response to JSON

        # 6. Upload the extracted data to the target S3 bucket
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
