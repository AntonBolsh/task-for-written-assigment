import unittest
from classes import CSVfile
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd


class TestSum(unittest.TestCase):

    def test_ideal_save(self):

        engine = create_engine("sqlite:///./data/test_function_data.db", echo=True, future=True)
        if not database_exists(engine.url):
            create_database(engine.url)
        conn = engine.connect()

        ideal_csv = CSVfile("ideal.csv", "ideal_functions", 1)
        ideal_csv.save_to_db(conn, 'ideal_functions')

        read_data_frame = pd.read_sql_table('ideal_functions', conn)

        self.assertEqual(self.data_frame, read_data_frame, "written data frame should be equal to read one")

if __name__ == '__main__':
    unittest.main()