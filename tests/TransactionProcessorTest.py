import unittest
import pandas as pd
from src.transaction_processor import TransactionProcessor

class TransactionProcessorTest(unittest.TestCase):

    # Initialize a test DataFrame
    def setUp(self):
        self.df = pd.DataFrame({
            'InvoiceNo': ['10001', '10002', '10003', 'C10004'],
            'StockCode': ['85123A', '84029G', '22749', '22197'],
            'Description': ['STARS GIFT TAPE', "POPPY'S PLAYHOUSE BEDROOM", 'HOME BUILDING BLOCK WORD', 'JUMBO STORAGE BAG SUKI'],
            'Quantity': [2, 5, 1, 3],
            'UnitPrice': [2.55, 4.95, 3.75, 1.65],
            'InvoiceDate': pd.to_datetime([
                '2011-01-10 10:00:00',
                '2011-02-15 15:00:00',
                '2011-01-20 10:30:00',
                '2011-03-01 11:00:00'
            ]),
            'CustomerID': [17850, 12583, 17897, 13047],
            'Country': ['France', 'United Kingdom', 'France', 'France']
        })
        self.processor = TransactionProcessor(self.df)
        self.processor.calculate_total_amount()

    # Check TotalAmount calcule
    def test_calculate_total_amount(self):
        self.assertIn("TotalAmount", self.processor.df.columns)
        expected = self.df["Quantity"] * self.df["UnitPrice"]
        pd.testing.assert_series_equal(
            self.processor.df["TotalAmount"].reset_index(drop=True),
            expected.reset_index(drop=True)
        )

    # Check that aggregation by country
    def test_group_by_country(self):
        result = self.processor.group_by_country()
        france_total = self.df[self.df["Country"] == "France"]
        expected = france_total["Quantity"] * france_total["UnitPrice"]
        france_sum = expected.sum()

        france_row = result[result["Country"] == "France"]
        self.assertAlmostEqual(france_row["TotalAmount"].values[0], france_sum)

    # Check that monthly stats 
    def test_aggregate_monthly_data(self):
        result = self.processor.aggregate_monthly_data()
        self.assertIn("total_sales", result.columns)
        self.assertIn("nb_transactions", result.columns)
        self.assertGreaterEqual(len(result), 1)

    # Check that the top product
    def test_calcul_stat_data(self):
        top_product, peak_hour = self.processor.calcul_stat_data()
        self.assertIsInstance(top_product, str)
        self.assertIsInstance(peak_hour, (int, float))

    # Check supplier
    def test_aggregate_supplier_data(self):
        supplier_df = pd.DataFrame({
            'StockCode': ['85123A', '84029G', '22749', '22197'],
            'Supplier': ['F001', 'F002', 'F001', 'F003']
        })

        global_rank, uk_rank = self.processor.aggregate_supplier_data(supplier_df)
        self.assertIn('F001', global_rank.index)
        self.assertTrue(global_rank.sum() > 0)
        self.assertTrue(uk_rank.sum() > 0)

    # Check continent 
    def test_aggregate_world_data(self):
        continent_df = pd.DataFrame({
            'Country': ['France', 'United Kingdom'],
            'Continent': ['Europe', 'Europe']
        })

        total_by_continent, cancel_by_continent = self.processor.aggregate_world_data(continent_df)

        self.assertIn("Europe", total_by_continent.index)
        self.assertIsInstance(total_by_continent["Europe"], float)
        self.assertIn("Europe", cancel_by_continent.index)
        self.assertIsInstance(cancel_by_continent["Europe"], (int, float))

if __name__ == '__main__':
    unittest.main()
