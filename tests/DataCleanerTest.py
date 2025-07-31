import unittest
import pandas as pd
from src.data_cleaner import DataCleaner

class DataCleanerTest(unittest.TestCase):
    
    # Initialize a test DataFrame
    def setUp(self):
        self.df = pd.DataFrame({
            'InvoiceNo': ['10001', '10002', 'C10003', '10002'],
            'StockCode': ['85123A', '84029G', '22749', '84029G'],
            'Description': ["STARS GIFT TAPE", "POPPY'S PLAYHOUSE BEDROOM", "HOME BUILDING BLOCK WORD", None],
            'Quantity': [5, 3, -1, 3],
            'UnitPrice': [2.55, 4.95, 3.75, 4.95],
            'CustomerID': [17850, None, 12583, 17897],
            'Country': ['France', 'Germany', 'France', 'Germany']
        })
        self.cleaner = DataCleaner(self.df)

    # Check that duplicates are properly removed
    def test_remove_duplicates(self):
        initial_count = len(self.df)
        self.cleaner.remove_duplicates()
        clean_df = self.cleaner.get_clean_data()
        self.assertLess(len(clean_df), initial_count)
        self.assertEqual(clean_df.duplicated().sum(), 0)

    # Check that missing values are handled correctly
    def test_handle_missing_values(self):
        self.cleaner.handle_missing_values()
        clean_df = self.cleaner.get_clean_data()
        self.assertFalse(clean_df["CustomerID"].isnull().any())
        self.assertFalse(clean_df["Description"].isnull().any())
        self.assertFalse(clean_df["Quantity"].isnull().any())

    # Check that cancelled transactions are filtered out
    def test_filter_valid_transactions(self):
        self.cleaner.filter_valid_transactions()
        clean_df = self.cleaner.get_clean_data()
        self.assertFalse(clean_df["InvoiceNo"].str.startswith("C").any())

if __name__ == '__main__':
    unittest.main()
