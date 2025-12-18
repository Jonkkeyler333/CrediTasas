import duckdb
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

class DuckDataLoader:
    def __init__(self, db_path: str, data_dir: str):
        self.db_path = db_path
        self.data_dir = data_dir
        self.conn = None

    def connect(self):
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")

    def _get_parquet_files(self):
        if not os.path.exists(self.data_dir):
            logger.warning(f"Data directory does not exist: {self.data_dir}")
            return []
        files = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
        return files

    def create_table(self):
        if self.conn is None:
            raise RuntimeError("Database connection is not established. Call connect() first")
        try:
            files = self._get_parquet_files()
            if not files:
                logger.warning("No parquet files found to create table schema")
                return
            files_list_str = ", ".join([f"'{f}'" for f in files])
            query = f"CREATE OR REPLACE TABLE tasas_creditos AS SELECT * FROM read_parquet([{files_list_str}], union_by_name=True) LIMIT 0;"
            
            self.conn.execute(query)
            logger.info("Table 'tasas_creditos' created/replaced successfully")
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def insert_data(self):
        if self.conn is None:
            raise RuntimeError("Database connection is not established. Call connect() first.")
        try:
            files = self._get_parquet_files()
            if not files:
                logger.warning("No parquet files found to insert.")
                return
            files_list_str = ", ".join([f"'{f}'" for f in files])
            query = f"INSERT INTO tasas_creditos SELECT * FROM read_parquet([{files_list_str}], union_by_name=True);"
            self.conn.execute(query)
            logger.info(f"Successfully loaded data from {len(files)} parquet files into 'tasas_creditos'")
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise