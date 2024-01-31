import unittest
import main
from main import load_data_from_json, data_quality_checks
import pandas as pd
from dotenv import load_dotenv
import os

class TestMainFunctions(unittest.TestCase):
    
    def setUp(self):
        load_dotenv()
        self.file_path = os.getenv("FILE_PATH")
        
    def test_load_data_from_json(self):
        df = load_data_from_json(self.file_path)
        self.assertIsInstance(df, pd.DataFrame) # Assert DataFrame type
        self.assertGreater(df.shape[0], 0) # Assert DataFrame has rows
    
    def test_valid_data(self):
        """Tests the function with valid data."""
        data = {
            'transactionId': [1, 2, 3],
            'currency': ['EUR', 'GBP', 'USD'],
            'transactionDate': ['2023-11-21', '2023-12-05', '2024-01-12']
        }
        df = pd.DataFrame(data)

        filtered_df, failed_df = data_quality_checks(df)

        self.assertEqual(len(filtered_df), 3)  # All rows should pass
        self.assertEqual(len(failed_df), 0)   # No rows should fail
    
    def test_invalid_currency(self):
        """Tests the function with invalid currency values."""
        data = {
            'transactionId': [1, 2, 3],
            'currency': ['CAD', 'JPY', 'USD'],  # 2 Invalid currencies
            'transactionDate': ['2023-11-21', '2023-12-05', '2024-01-12']
        }
        df = pd.DataFrame(data)

        filtered_df, failed_df = data_quality_checks(df)

        self.assertEqual(len(filtered_df), 1)  # Only USD row should pass
        self.assertEqual(len(failed_df), 2)   # CAD and JPY rows should fail


if __name__ == '__main__':
    unittest.main()
