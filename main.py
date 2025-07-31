
import logging
import pandas as pd
from src.etl_pipeline import ETLPipeline
from utils.functions import read_file


logging.basicConfig(level=logging.INFO)

df = read_file("data/Online_Retail_silver.xlsx")
supplier_df = read_file("data/Supplier.csv")


pipeline = ETLPipeline(df, suppliers, continents)
pipeline.run_pipeline()
#pipeline.save_as_parquet("data/final.parquet")



