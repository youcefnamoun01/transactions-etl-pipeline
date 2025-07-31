import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")


# Configurer ton client S3 avec tes credentials AWS
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def load_csv_from_s3(bucket_name, file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])

def load_excel_from_s3(bucket_name, file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_excel(BytesIO(obj['Body'].read()), engine='openpyxl')

def upload_xlsx_to_s3(file_path, bucket_name, s3_key):
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Fichier uploadé : s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Erreur lors de l'upload : {str(e)}")



""""
# Exemple d’utilisation :
bucket = "projet-data-storage"
csv_key = "Supplier.csv"
excel_key = "Online_Retail_bronze.xlsx"

upload_xlsx_to_s3("../data/Online_Retail_silver.xlsx", bucket, "Online_Retail_silver.xlsx")


df_csv = load_csv_from_s3(bucket, csv_key)

df_excel = load_excel_from_s3(bucket, excel_key)

print(df_csv.head())
print(df_excel.head())
"""