import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import os
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db_url:str, data_dir:str):
        self.engine = create_engine(db_url)
        self.data_dir = data_dir
    
    def _get_parquet_files(self):
        if not os.path.exists(self.data_dir):
            logger.warning(f"Data directory does not exist: {self.data_dir}")
            return []
        return [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
    
    def create_table(self):
        try:
            paths = self._get_parquet_files()
            if not paths:
                logger.error("No parquet files found")
                raise FileNotFoundError(f"No parquet files in {self.data_dir}")
            batch=pd.read_parquet(paths[0])
            batch.head(0).to_sql('tasas_creditos',self.engine,if_exists='replace',index=False)
            logger.info("Table 'tasas_creditos' created successfully")
        except FileNotFoundError as e:
            logger.error(f"Parquet file not found: {e}")
            raise
        except (SQLAlchemyError, OperationalError) as e:
            logger.error(f"Database error creating table: {e}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing parquet file: {e}")
            raise
    
    def insert_data(self):
        paths = self._get_parquet_files()
        if not paths:
            logger.warning("No parquet files found to insert")
            return
        for path in paths:
            try:
                batch = pd.read_parquet(path)
                batch.to_sql('tasas_creditos', self.engine, if_exists='append', index=False)
                logger.info(f"Data from {path} inserted successfully")
            except FileNotFoundError as e:
                logger.error(f"Parquet file not found at {path}: {e}")
                continue
            except (SQLAlchemyError, OperationalError) as e:
                logger.error(f"Database error inserting data from {path}: {e}")
                raise
            except pd.errors.ParserError as e:
                logger.error(f"Error parsing parquet file {path}: {e}")
                continue
            except ValueError as e:
                logger.error(f"Data validation error in {path}: {e}")
                continue