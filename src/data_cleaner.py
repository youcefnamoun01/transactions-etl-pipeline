import logging
class DataCleaner:
    def __init__(self, df):
        self.df = df

    # Remove duplications
    def remove_duplicates(self):
        logging.info("Suppression des doublons")
        self.df = self.df.drop_duplicates()

    # Remove missing values
    def handle_missing_values(self):
        logging.info("Traitement des valeurs manquantes")
        self.df = self.df.dropna(subset=["CustomerID", "Description", "Quantity", "UnitPrice"])
    
    # Remove canceled transactions
    def filter_valid_transactions(self):
        logging.info("Filtrage des transactions annulées")
        self.df = self.df[self.df['InvoiceNo'].str[0] != 'C']

    # Return cleaned dataset
    def get_clean_data(self):
        return self.df

    # Save cleaned dataset as silver data
    def save_clean_data(self):
        logging.info("Sauvegarde du fichier aprés nettoyage")
        self.df.to_excel("data/Online_Retail_silver.xlsx", index=False)