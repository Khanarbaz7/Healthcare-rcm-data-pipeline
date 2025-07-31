from db_connector import get_mysql_engine
import pandas as pd

class DataExtractor:
    def __init__(self, db_name):
        self.engine = get_mysql_engine(db_name)

    def extract_table(self, table_name, source=None):
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
            if source:
                df["HospitalID"] = source
            print(f"[{table_name}] → Extracted {len(df)} records")
            return df
        except Exception as e:
            print(f"Error extracting {table_name}: {e}")
            return pd.DataFrame()

