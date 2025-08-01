import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")


# Configuration credentials AWS
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def load_csv_from_s3(bucket_name, file_key):
    logging.info(f"Chargement du fichier {file_key} depuis AWS S3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])

def load_excel_from_s3(bucket_name, file_key):
    logging.info(f"Chargement du fichier {file_key} depuis AWS S3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_excel(BytesIO(obj['Body'].read()), engine='openpyxl')


def upload_to_s3(df, bucket_name, s3_key):
    try:
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        
        s3.upload_fileobj(buffer, bucket_name, s3_key)
        print(f"Fichier uploadé : s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Erreur lors de l'upload : {str(e)}")


def upload_to_s3_as_parquet(df, bucket_name: str, prefix: str):
    try:
        df["date"] = pd.to_datetime(df["InvoiceDate"]).dt.date
        today_str = datetime.today().strftime("%Y-%m-%d")
        partition_path = f"date={today_str}"
        filename = "Online_Retail_gold.parquet"
        s3_key = f"{prefix}/{partition_path}/{filename}"
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket_name, s3_key)
        logging.info(f"Parquet sauvegardé avec partition date : s3://{bucket_name}/{s3_key}")

    except Exception as e:
        logging.error(f"Erreur upload parquet partitionné : {str(e)}")
