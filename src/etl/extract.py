import logging
import requests
import pandas as pd
from typing import Generator
import time

logger = logging.getLogger(__name__)

class ETLExtractor:
    """ ETLExtractor is responsible for extracting data from a specified source API
    """
    def __init__(self, source: str):
        self.source = source
        logger.info(f"ETLExtractor initialized with source: {source}")
    
    def extract_batch(self, limit:int=50000, offset:int=1, column_order:str="fecha_corte", api_token:str="")-> pd.DataFrame:
        """ Extract a batch of data from the source API

        :param limit: the size of the batch (number of records) to fetch, defaults to 50000
        :type limit: int, optional
        :param offset: the starting point in the dataset to fetch records from, defaults to 0
        :type offset: int, optional
        :param column_order: the column name to order the data by, defaults to "fecha_corte"
        :type column_order: str, optional
        :param api_token: the API token for authentication, defaults to ""
        :type api_token: str, optional
        :return: a DataFrame containing the extracted batch of data
        :rtype: pd.DataFrame
        """
        logger.debug(f"Fetching batch: offset={offset}, limit={limit}")
        r=requests.get(f"{self.source}?pageSize={limit}&pageNumber={offset}&app_token={api_token}&query=SELECT * ORDER BY {column_order}")
        r.raise_for_status()
        data = pd.DataFrame.from_records(r.json())
        logger.info(f"Batch fetched successfully: {len(data)} records")
        return data
    
    def extract_all(self, batch_size:int=50000, column_order:str="fecha_corte", api_token:str="", path:str="")-> None:
        """ Extract all data from the source API in batches
        :param batch_size: the size of each batch to fetch, defaults to 50000
        :type batch_size: int, optional
        :param column_order: the column name to order the data by, defaults to "fecha_corte"
        :type column_order: str, optional
        :param api_token: the API token for authentication, defaults to ""
        :type api_token: str, optional
        :param path: the directory to save each batch as a parquet file, defaults to ""
        :type path: str, optional
        :yield: a DataFrame containing each extracted batch of data
        :rtype: Generator[pd.DataFrame, None, None]
        """
        batch_num = 1
        offset = 1
        while True:
            try:
                batch = self.extract_batch(limit=batch_size, offset=offset, column_order=column_order, api_token=api_token)
                if batch.empty:
                    logger.info("No more data to extract. Stopping")
                    break
                if path:
                    batch.to_parquet(f"{path}/batch_{batch_num}.parquet")
                    logger.info(f"Batch {batch_num} saved to {path}/batch_{batch_num}.parquet")
                batch_num += 1
                offset += 1
                time.sleep(3)  # to avoid hitting rate limits
            except ConnectionError as e:
                logger.error(f"Connection error occurred: {e}")
                raise
            except requests.HTTPError as e:
                logger.error(f"HTTP error occurred: {e}")
                raise
        logger.info(f"Total batches extracted: {batch_num}")