import pandas as pd
import logging
from utils.countries_api import get_continent_from_country

class TransactionProcessor:
    def __init__(self, df):
        self.df = df.copy()
    
    # Calculate total transaction amount
    def calculate_total_amount(self):
        logging.info("Calcule des montants des transactions")
        self.df["TotalAmount"] = self.df["Quantity"] * self.df["UnitPrice"]
        logging.info("\n%s", self.df)
    
    # Group and sum TotalAmount by country
    def group_by_country(self):
        logging.info("Regroupement des montants par pays")
        grouped_df = self.df.groupby("Country")["TotalAmount"].sum().reset_index()
        logging.info("\n%s", grouped_df)
        return grouped_df

    # Calculate monthly sum : total amount, quantity, number of transactions
    def aggregate_monthly_data(self):
        logging.info("Agrégation mensuelle")
        self.df["InvoiceDate"] = pd.to_datetime(self.df["InvoiceDate"])
        self.df["YearMonth"] = self.df["InvoiceDate"].dt.to_period("M")
        monthly_df = self.df.groupby("YearMonth").agg(
            total_sales=("TotalAmount", "sum"),
            total_quantities=("Quantity", "sum"),
            nb_transactions=("InvoiceNo", "nunique")
        ).reset_index()
        logging.info("\n%s", monthly_df)
        return monthly_df
    
    # Calculate data statistics
    def calcul_stat_data(self):
        logging.info("Analyse des données")
        logging.info("Le Produit le plus rentable en France")
        df_france = self.df[self.df["Country"] == "France"]
        top_product_data = (
            df_france.groupby("Description")["TotalAmount"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .iloc[0]
        )
        top_product = top_product_data["Description"]
        total_amount = top_product_data["TotalAmount"]
        logging.info(f"Produit : {top_product} | TotalAmount : {total_amount:.2f}")
       
        logging.info("Heure avec le plus de transactions")
        self.df["Hour"] = self.df["InvoiceDate"].dt.hour
        top_hour = self.df.groupby("Hour")["InvoiceNo"].count().idxmax()
        logging.info(f"{top_hour}H")
        return top_product, top_hour

    # Calculate ranking of suppliers with total sales
    def aggregate_supplier_data(self, supplier_df):
        df = self.df.copy()
        # Supprimer les transations annulées
        supplier_df = supplier_df[supplier_df['InvoiceNo'].str[0] != 'C']
        df["InvoiceNo"] = df["InvoiceNo"].astype(str)
        supplier_df["InvoiceNo"] = supplier_df["InvoiceNo"].astype(str)
        df = df.merge(supplier_df, on="InvoiceNo", how="left")
        supplier_rank = df.groupby("Fournisseur")["TotalAmount"].sum().sort_values(ascending=False)
        logging.info(f"Classement fournisseurs \n {supplier_rank}")
        uk_2011 = df[(df["Country"] == "United Kingdom") & (df["InvoiceDate"].dt.year == 2011)]
        uk_rank = uk_2011.groupby("Fournisseur")["TotalAmount"].sum().sort_values(ascending=False)
        logging.info(f"Classement fournisseurs à United Kingdom en 2011 \n {supplier_rank}")
        return supplier_rank, uk_rank

    # Aggregate data by continents
    def aggregate_world_data(self):
        df = self.df.copy()
        df["Continent"] = df["Country"].apply(get_continent_from_country)
        print(df)
        total_by_continent = (
            df.groupby("Continent")["TotalAmount"]
            .sum()
            .sort_values(ascending=False)
        )
        logging.info(f"Classement des continents selon les dépenses effectué \n {total_by_continent}")

        df["IsCancelled"] = df["InvoiceNo"].astype(str).str.startswith("C")
        cancel_by_continent = (
            df[df["IsCancelled"]].groupby("Continent")["InvoiceNo"]
            .count()
            .sort_values(ascending=False)
        )
        logging.info(f"Classement des continents par nombre d'annulations effectué \n {cancel_by_continent}")
        return total_by_continent, cancel_by_continent