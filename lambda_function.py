import logging
from src.etl_pipeline import ETLPipeline
from utils.aws_s3_connect import (
    load_csv_from_s3,
    load_excel_from_s3,
    upload_to_s3_as_parquet
)

logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    try:
        
        bucket_name = "projet-data-storage"
        retail_key = "Online_Retail_bronze.xlsx"
        suppliers_key = "Supplier.csv"

        # Chargement des données depuis S3
        retail_df = load_excel_from_s3(bucket_name, retail_key)
        suppliers_df = load_csv_from_s3(bucket_name, suppliers_key)
        print(retail_df)
        print(suppliers_df)

        # Pipeline
        pipeline = ETLPipeline(retail_df, suppliers_df)
        pipeline.run_pipeline()
        pipeline.save_as_parquet(bucket_name, "Online_Retail_gold")

        return {
            "statusCode": 200,
            "message": "ETL terminé avec succès"
        }

    except Exception as e:
        logging.error(f"Erreur dans le handler Lambda : {e}")
        return {
            "statusCode": 500,
            "message": f"Erreur : {str(e)}"
        }
