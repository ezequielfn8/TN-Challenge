import os
import sys
import requests
import pandas as pd
import snowflake.connector
import time
import io
import logging
from awsglue.utils import getResolvedOptions

MSG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Library calls ok")


class DataExtractor:
    def __init__(self):
        params = [
            "BASE_URL", 
            "SNOWFLAKE_USER", 
            "SNOWFLAKE_PASSWORD", 
            "SNOWFLAKE_ACCOUNT", 
            "SNOWFLAKE_WAREHOUSE", 
            "SNOWFLAKE_DATABASE", 
            "SNOWFLAKE_SCHEMA", 
            "RAW_TABLE"
        ]
        self.args = getResolvedOptions(sys.argv, params)
        
        self.base_url = self.args["BASE_URL"]
        self.snowflake_user = self.args["SNOWFLAKE_USER"]
        self.snowflake_password = self.args["SNOWFLAKE_PASSWORD"]
        self.snowflake_account = self.args["SNOWFLAKE_ACCOUNT"]
        self.snowflake_warehouse = self.args["SNOWFLAKE_WAREHOUSE"]
        self.snowflake_database = self.args["SNOWFLAKE_DATABASE"]
        self.snowflake_schema = self.args["SNOWFLAKE_SCHEMA"]
        self.raw_table = self.args["RAW_TABLE"]
        logger.info("DataExtractor initialized with Glue job parameters.")

    def fetch_data(self):
        """Fetches all data from the API with pagination and returns a DataFrame."""
        params = {
            "$limit": 1000,
            "$offset": 0
        }
        all_data = []

        while True:
            response = requests.get(self.base_url, params=params)

            if response.status_code == 200:
                data = pd.read_csv(io.StringIO(response.text))
                if data.empty or params["$offset"] >= 10000:  # Limit for testing
                    logger.info("Data fetch limit reached or no more data.")
                    break
                all_data.append(data)
                params["$offset"] += params["$limit"]
                time.sleep(1)  # Avoid API rate limits
            else:
                logger.error(f"Failed to fetch data: {response.status_code}")
                break

        logger.info(f"Fetched {len(all_data)} batches of data.")
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def load_data_to_snowflake(self, df):
        """Loads the data into Snowflake staging table."""
        conn = snowflake.connector.connect(
            user=self.snowflake_user,
            password=self.snowflake_password,
            account=self.snowflake_account,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema
        )
        cursor = conn.cursor()
        logger.info("Connected to Snowflake.")

        columns = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))

        for row in df.itertuples(index=False, name=None):
            cursor.execute(
                f"INSERT INTO {self.raw_table} ({columns}) VALUES ({placeholders})",
                row
            )

        cursor.close()
        conn.close()
        logger.info("Data successfully loaded into Snowflake.")

    def run(self):
        """Orchestrate the data extraction and loading process."""
        logger.info("Starting data extraction from API...")
        df = self.fetch_data()

        if df.empty:
            logger.warning("No data fetched from API.")
            return

        logger.info(f"Fetched {len(df)} records.")
        logger.info("Loading data into Snowflake...")
        self.load_data_to_snowflake(df)
        logger.info("Data extraction and loading process completed.")


if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.run()
