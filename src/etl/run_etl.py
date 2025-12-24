import logging
from .extract import ETLExtractor
from .transform import ETLTransformer
from .load_duck import DuckDataLoader
import os
from pathlib import Path
from src.config import config

log_dir = Path(__file__).parent / "logs"
log_filename = "etl_process.log"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, log_filename),
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

print(config.socrata.URL)

def run_etl():
    logging.info("ETL process started")
    logging.info("Starting Extraction Phase")
    extractor = ETLExtractor(source=config.socrata.URL)
    data_dir = config.DATA_DIR / "raw"
    os.makedirs(data_dir, exist_ok=True)
    extractor.extract_all(api_token=config.socrata.APP_TOKEN, path=str(data_dir))
    logging.info("Extraction Phase completed")
    logging.info("Starting Transformation Phase")
    transformer = ETLTransformer()
    clean_data_dir = config.DATA_DIR / "clean"
    os.makedirs(clean_data_dir, exist_ok=True)
    transformer.transform_all(raw_data_dir=str(data_dir), output_dir=str(clean_data_dir))
    logging.info("Transformation Phase completed")
    logging.info("Starting Loading Phase")
    loader = DuckDataLoader(db_path=config.db.duckdb_path, data_dir=str(clean_data_dir))
    try:
        loader.connect()
        loader.create_table()
        loader.insert_data()
    except Exception as e:
        logging.error(f"Loading Phase failed: {e}")
        raise
    finally:
        loader.close()
    logging.info("Loading Phase completed")
    logging.info("ETL process finished successfully")

if __name__ == "__main__":
    run_etl()    