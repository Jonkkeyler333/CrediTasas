import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
MODELS_DIR = BASE_DIR / 'models'
load_dotenv(BASE_DIR / '.env')

class DatabaseConfig:
    HOST = os.getenv('DB_HOST', 'localhost')
    PORT = os.getenv('DB_PORT', '5433')
    USER = os.getenv('DB_USER', 'credituser')
    PASSWORD = os.getenv('DB_PASSWORD', 'creditpass123')
    NAME = os.getenv('DB_NAME', 'credit_db')
    
    @property
    def url(self):
        return f'postgresql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.NAME}'
    
    @property
    def async_url(self):
        return f'postgresql+asyncpg://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.NAME}'

    @property
    def duckdb_path(self):
        return str(Config.DATA_DIR / 'credit_db.duckdb')

class SocrataConfig:
    URL = os.getenv('URL', 'https://www.datos.gov.co/resource/qzsc-9esp.json')
    APP_TOKEN = os.getenv('TOKEN' , '6gPu48u90tDGg4i9H6nCLqddA') 

class Config:
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

    db = DatabaseConfig()
    socrata = SocrataConfig()
    
    BASE_DIR = BASE_DIR
    DATA_DIR = DATA_DIR
    MODELS_DIR = MODELS_DIR

config = Config()