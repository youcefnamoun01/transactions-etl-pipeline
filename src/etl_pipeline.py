import logging
from etl.data_cleaner import DataCleaner
from etl.transaction_processor import TransactionProcessor

class ETLPipeline:
    def __init__(self, df, supplier_df):
        self.df = df
        self.supplier_df = supplier_df

    def run_pipeline(self):
        logging.info("Lancement du pipeline ETL")

        # Nettoyage
        cleaner = DataCleaner(self.df)
        cleaner.remove_duplicates()
        cleaner.handle_missing_values()
        cleaner.filter_valid_transactions()
        clean_df = cleaner.get_clean_data()

        # Traitement
        processor = TransactionProcessor(clean_df)
        processor.calculate_total_amount()
        processor.group_by_country()
        processor.aggregate_monthly_data()
        processor.calcul_stat_data()
        processor.aggregate_supplier_data(self.supplier_df)
        processor.aggregate_world_data()

        self.df = processor.df

    def save_as_parquet(self, path: str):
        pass
