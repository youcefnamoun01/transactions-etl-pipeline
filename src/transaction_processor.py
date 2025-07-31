import pandas as pd
import logging
from utils.countries_api import get_continent_from_country

class TransactionProcessor:
    def __init__(self, df):
        #self.df = df.copy().head(10000)
        self.df = df
    def calculate_total_amount(self):
        logging.info("Calcule des montants des transactions")
        self.df["TotalAmount"] = self.df["Quantity"] * self.df["UnitPrice"]
    
    def group_by_country(self):
        logging.info("Regroupement par pays")
        return self.df.groupby("Country")["TotalAmount"].sum().reset_index()

    def aggregate_monthly_data(self):
        logging.info("Agrégation mensuelle")
        self.df["InvoiceDate"] = pd.to_datetime(self.df["InvoiceDate"])
        self.df["YearMonth"] = self.df["InvoiceDate"].dt.to_period("M")
        return self.df.groupby("YearMonth").agg(
            total_sales=("TotalAmount", "sum"),
            total_quantities=("Quantity", "sum"),
            nb_transactions=("InvoiceNo", "nunique")
        ).reset_index()
    
    def calcul_stat_data(self):
        logging.info("Analyse des données...")

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
       
        logging.info("Nombre de transactions par heure")
        self.df["Hour"] = self.df["InvoiceDate"].dt.hour
        hourly_transactions = self.df.groupby("Hour").agg(
            nb_transactions=("InvoiceNo", "nunique")
        ).reset_index().sort_values(by="nb_transactions", ascending=False)
        logging.info(hourly_transactions)

    def aggregate_supplier_data(self, supplier_df):
        df = self.df.copy()
        supplier_df = supplier_df[supplier_df['InvoiceNo'].str[0] != 'C']
        df["InvoiceNo"] = df["InvoiceNo"].astype(str)
        supplier_df["InvoiceNo"] = supplier_df["InvoiceNo"].astype(str)
        df = df.merge(supplier_df, on="InvoiceNo", how="left")
        global_rank = df.groupby("Fournisseur")["TotalAmount"].sum().sort_values(ascending=False)
        uk_2011 = df[(df["Country"] == "United Kingdom") & (df["InvoiceDate"].dt.year == 2011)]
        uk_rank = uk_2011.groupby("Fournisseur")["TotalAmount"].sum().sort_values(ascending=False)
        print(uk_rank)
        return global_rank, uk_rank

    def aggregate_world_data(self):
        df = self.df.copy().head(10)

        df["Continent"] = df["Country"].apply(get_continent_from_country)
        print(df)
        total_by_continent = (
            df.groupby("Continent")["TotalAmount"]
            .sum()
            .sort_values(ascending=False)
        )
        logging.info("Classement des continents selon les dépenses effectué.")

        df["IsCancelled"] = df["InvoiceNo"].astype(str).str.startswith("C")
        cancel_by_continent = (
            df[df["IsCancelled"]].groupby("Continent")["InvoiceNo"]
            .count()
            .sort_values(ascending=False)
        )
        logging.info("Classement des continents par nombre d'annulations effectué.")
        return total_by_continent, cancel_by_continent