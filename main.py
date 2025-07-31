
import logging
import pandas as pd
#from src.etl_pipeline import ETLPipeline
from utils.functions import read_file
from utils.aws_s3_connect import load_csv_from_s3, load_excel_from_s3


logging.basicConfig(level=logging.INFO)

bucket = "projet-data-storage"
csv_key = "Supplier.csv"
excel_key = "Online_Retail_bronze.xlsx"

df_online_retail = load_excel_from_s3(bucket, excel_key)
df_suppliers = load_csv_from_s3(bucket, csv_key)
print(df_online_retail)
print(df_suppliers)

pipeline = ETLPipeline(df, suppliers, continents)
pipeline.run_pipeline()
#pipeline.save_as_parquet("data/final.parquet")


"""
df = read_file("data/Online_Retail_silver.xlsx")
supplier_df = read_file("data/Supplier.csv")
"""