# Use the ListFoundationModels API to show the models that are available in your region.
import boto3
             
# Create an &BR; client in the &region-us-east-1; Region.
bedrock = boto3.client(
    service_name="bedrock"
)

bedrock.list_foundation_models()