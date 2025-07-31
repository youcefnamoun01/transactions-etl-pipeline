
import logging
import pandas as pd
from src.transaction_processor import TransactionProcessor
from utils.functions import read_file

logging.basicConfig(level=logging.INFO)

df = read_file("data/Online_Retail_silver.xlsx")
supplier_df = read_file("data/Supplier.csv")

print(supplier_df)


transaction_process = TransactionProcessor(df)
transaction_process.calculate_total_amount()
transaction_process.group_by_country()
transaction_process.aggregate_monthly_data()
transaction_process.calcul_stat_data()
transaction_process.aggregate_supplier_data(supplier_df)



