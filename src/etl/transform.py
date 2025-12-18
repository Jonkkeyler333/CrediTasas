import logging
import pandas as pd
from typing import Generator
import os
import time
from src.config import config

logger = logging.getLogger(__name__)

class ETLTransformer:
    def __init__(self):
        self.codigos_municipios = self._load_municipio_codes()
    
    def _load_municipio_codes(self) ->pd.DataFrame:
        df = pd.read_csv(config.DATA_DIR / "DIVIPOLA_codigos_municipios_2025.csv")
        logger.info("Municipio codes loaded successfully")
        return df[['Código Municipio', 'Nombre Departamento', 'Nombre Municipio']]
    
    def clean_batch(self, batch: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Cleaning batch with {len(batch)} records")
        batch = batch.drop(columns=[":id", ":version", ":created_at", ":updated_at"], errors='ignore')
        batch['tipo_entidad'] = pd.to_numeric(batch['tipo_entidad'], errors='coerce', downcast='integer')
        batch['codigo_entidad'] = pd.to_numeric(batch['codigo_entidad'], errors='coerce', downcast='integer')
        batch['codigo_municipio'] = pd.to_numeric(batch['codigo_municipio'], errors='coerce', downcast='integer')
        batch['codigo_ciiu'] = pd.to_numeric(batch['codigo_ciiu'], errors='coerce', downcast='integer')
        batch['numero_de_creditos'] = pd.to_numeric(batch['numero_de_creditos'], errors='coerce', downcast='unsigned')
        batch['tasa_efectiva_promedio'] = pd.to_numeric(batch['tasa_efectiva_promedio'], errors='coerce', downcast='float')
        batch['margen_adicional_a_la'] = pd.to_numeric(batch['margen_adicional_a_la'], errors='coerce', downcast='float')
        batch['montos_desembolsados'] = pd.to_numeric(batch['montos_desembolsados'], errors='coerce', downcast='float')
        batch['fecha_corte'] = pd.to_datetime(batch['fecha_corte'], errors='coerce', format='%Y-%m-%d')
        categoricas_simples = [
            'tipo_de_persona',
            'sexo',
            'tipo_de_cr_dito',
            'tipo_de_garant_a',
            'producto_de_cr_dito',
            'tipo_de_tasa',
            'clase_deudor',
            'grupo_etnico',
            'nombre_tipo_entidad',
            'tama_o_de_empresa',
            'plazo_de_cr_dito',
            'rango_monto_desembolsado',
            'antiguedad_de_la_empresa'
        ]
        for col in categoricas_simples:
            if col in batch.columns:
                batch[col] = batch[col].astype('category')
        logger.info(f"Batch cleaned: {len(batch)}")
        return batch

    def transform_all(self,raw_data_dir:str, output_dir:str) -> None:
        batches = [os.path.join(raw_data_dir, f) for f in os.listdir(raw_data_dir) if f.endswith('.parquet')]
        for i,batch in enumerate(batches):
            df = pd.read_parquet(batch)
            clean_batch = self.clean_batch(df)
            clean_batch = clean_batch.merge(self.codigos_municipios, how='left', left_on='codigo_municipio', right_on='Código Municipio')
            clean_batch.rename(columns={'Nombre Departamento': 'nombre_departamento','Nombre Municipio': 'nombre_municipio'}, inplace=True)
            clean_batch.drop(columns=['Código Municipio'], inplace=True)
            clean_batch.to_parquet(f"{output_dir}/batch_{i}_clean.parquet")
            logging.info(f"Transformed batch {i} saved to {output_dir}/batch_{i}_clean.parquet")