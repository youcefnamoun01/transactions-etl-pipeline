import logging
from utils.aws_s3_connect import upload_to_s3
import os

bucket = os.getenv("BUCKET_NAME")

class DataCleaner:
    def __init__(self, df):
        self.df = df

    # Remove duplications
    def remove_duplicates(self):
        logging.info("Suppression des doublons")
        duplicates = self.df[self.df.duplicated(keep='first')]
        logging.info(f"{len(duplicates)} lignes en doubles trouvées")
        self.df = self.df.drop_duplicates()
        upload_to_s3(duplicates, bucket, "reporting/duplicate_data.xlsx")

    # Remove missing values
    def handle_missing_values(self):
        logging.info("Traitement des valeurs manquantes")
        cols = ["CustomerID", "Description", "Quantity", "UnitPrice"]
        missing_data = self.df[self.df[cols].isnull().any(axis=1)]
        nb_missing = len(missing_data)
        logging.info(f"{nb_missing} valeurs manquantes trouvées")
        self.df = self.df.dropna(subset=cols)
        upload_to_s3(missing_data, bucket, "reporting/missing_data.xlsx")

    
    # Remove canceled transactions
    def filter_valid_transactions(self):
        logging.info("Filtrage des transactions annulées")
        cancelled = self.df[self.df['InvoiceNo'].astype(str).str.startswith('C')]
        nb_cancelled = len(cancelled)
        logging.info(f"{nb_cancelled} transactions annulées trouvées")
        self.df = self.df[~self.df['InvoiceNo'].astype(str).str.startswith('C')]
        upload_to_s3(cancelled, bucket, "reporting/cancelled_transactions.xlsx")


    # Return cleaned dataset
    def get_clean_data(self):
        return self.df

    # Save cleaned dataset as silver data
    def save_clean_data(self):
        logging.info("Sauvegarde du fichier aprés nettoyage")
        upload_to_s3(self.df, bucket, "Online_Retail_silver.xlsx")