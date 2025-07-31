
import logging
import pandas as pd
from src.etl_pipeline import ETLPipeline
from utils.functions import read_file
from utils.aws_s3_connect import load_csv_from_s3, load_excel_from_s3


logging.basicConfig(level=logging.INFO)

"""
bucket = "projet-data-storage"
csv_key = "Supplier.csv"
excel_key = "Online_Retail_bronze.xlsx"

df_online_retail = load_excel_from_s3(bucket, excel_key)
df_suppliers = load_csv_from_s3(bucket, csv_key)
print(df_online_retail)
print(df_suppliers)
"""

bucket = "projet-data-storage"
df = read_file("data/Online_Retail_silver.xlsx")
suppliers = read_file("data/Supplier.csv")

pipeline = ETLPipeline(df, suppliers)
pipeline.run_pipeline()
pipeline.save_as_parquet(bucket, "Online_Retail_gold.parquet")

