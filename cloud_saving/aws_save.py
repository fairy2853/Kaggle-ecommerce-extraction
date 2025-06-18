import boto3
import os
from dotenv import load_dotenv

load_dotenv()


def aws_save():

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    bucket_name = "my-data-engineering-bucket"
    s3.upload_file("output/sales_summary.csv", bucket_name, "sales/sales_summary.csv")
    print("✅ file uploaded to S3")
