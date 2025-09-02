"""
Script principal del pipeline ETL de COVID-19.

Este módulo orquesta el flujo completo:
1️⃣ Extracción de datos desde la API COVID.
2️⃣ Transformación y limpieza de datos.
3️⃣ Guardado local en formato Parquet.
4️⃣ Subida de los archivos a AWS S3.

El pipeline se ejecuta para todos los países definidos en la variable COUNTRIES
y utiliza la fecha definida en COVID_DATE.

Requisitos:
- Configuración en .env (API, AWS, países, fecha, umbrales de riesgo)
- Funciones auxiliares en extract.py, transform.py, load.py y utils.py
"""
import logging
from datetime import datetime, timedelta
from extract import fetch_country_confirmed
from transform import process_country_df
from load import save_local_parquet, upload_to_s3
from utils import setup_logging
from config import COUNTRIES, COVID_DATE, RISK_THRESHOLDS, S3_BUCKET_NAME, AWS_DEFAULT_REGION

logger = logging.getLogger("etl")

def run():
    """
    Ejecuta el pipeline ETL completo para todos los países definidos.

    Flujo:
    1. Extrae datos COVID de la API para cada país y fecha.
    2. Procesa y transforma los datos mediante `process_country_df`.
    3. Guarda los datos transformados en formato Parquet localmente.
    4. Sube los archivos Parquet a AWS S3.

    Manejo de errores:
    - Registra cualquier fallo durante el pipeline para un país específico.
    - Continúa con el siguiente país aunque falle alguno.
    
    """
    for iso in COUNTRIES:
        try:
            logger.info("Iniciando pipeline para %s — %s a %s", iso, COVID_DATE)
            # 1️⃣ Extracción
            df_raw = fetch_country_confirmed(iso, COVID_DATE)
            # 2️⃣ Transformación
            df_transformed, totals = process_country_df(df_raw, iso, RISK_THRESHOLDS)
            # 3️⃣ Guardado local
            local_path = save_local_parquet(df_transformed, iso, COVID_DATE)
            # 4️⃣ Subida a S3
            s3_key = f"covid_data/{iso}/{COVID_DATE}.parquet"
            try:
                upload_to_s3(local_path, S3_BUCKET_NAME, s3_key, aws_region=AWS_DEFAULT_REGION)
                logger.info("Archivo subido a S3: %s", s3_key)
            except Exception as e:
                logger.exception("Error al subir %s a S3: %s", iso, e)
        except Exception:
            logger.exception("Fallo en pipeline para %s", iso)

if __name__ == "__main__":
    run()
