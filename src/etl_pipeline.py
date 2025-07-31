import logging
from src.data_cleaner import DataCleaner
from src.transaction_processor import TransactionProcessor
from utils.functions import read_file
from utils.aws_s3_connect import upload_to_s3_as_parquet
from io import BytesIO

class ETLPipeline:
    def __init__(self, df, supplier_df):
        self.df = df
        self.supplier_df = supplier_df

    def run_pipeline(self):
        logging.info("--------Lancement du pipeline ETL--------")
        """
        logging.info("Nettoyage des données...")
        cleaner = DataCleaner(self.df)
        cleaner.remove_duplicates()
        cleaner.handle_missing_values()
        cleaner.filter_valid_transactions()
        clean_df = cleaner.get_clean_data()
        logging.info(f"Dataframe aprés nettoyage {self.df}")
        """
        clean_df = read_file("data/Online_Retail_silver.xlsx")

        logging.info("Traitement des données...")
        processor = TransactionProcessor(clean_df)
        processor.calculate_total_amount()
        processor.group_by_country()
        processor.aggregate_monthly_data()
        processor.calcul_stat_data()
        processor.aggregate_supplier_data(self.supplier_df)
        processor.aggregate_world_data()
        self.df = processor.df

    def save_as_parquet(self, bucket_name: str, s3_key: str):
        self.df["StockCode"] = self.df["StockCode"].astype(str)
        upload_to_s3_as_parquet(self.df, bucket_name, s3_key)

