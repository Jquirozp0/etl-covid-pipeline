"""
Módulo de carga de datos (Load) para el pipeline ETL.

Este módulo contiene funciones para:
- Guardar DataFrames en formato Parquet localmente.
- Subir archivos a un bucket de AWS S3.

Funciones:
- save_local_parquet: Guarda un DataFrame como parquet en la carpeta correspondiente.
- upload_to_s3: Sube un archivo local a AWS S3.
"""
import os
import logging
import boto3
import pandas as pd

# Logger específico para la etapa de carga
logger = logging.getLogger("etl.load")

def save_local_parquet(df: pd.DataFrame, country: str, date_label: str, base_path="data"):
    """
    Guarda un DataFrame en formato Parquet localmente.

    Args:
        df (pd.DataFrame): DataFrame a guardar.
        country (str): Código ISO del país (ej. "MX", "CO").
        date_label (str): Etiqueta de fecha para el archivo (ej. 'YYYY-MM-DD').
        base_path (str, opcional): Carpeta base donde guardar los archivos. Por defecto 'data'.

    Returns:
        str: Ruta completa del archivo Parquet guardado.

    Comportamiento:
        - Crea automáticamente la carpeta correspondiente si no existe.
        - Guarda el archivo como "{date_label}.parquet" dentro de "base_path/{country}/".
        - Registra información en el logger sobre la operación.
    
    Ejemplo:
        >>> df = pd.DataFrame({"a": [1,2]})
        >>> save_local_parquet(df, "MX", "2023-09-01")
        'data/MX/2023-09-01.parquet'
    """
    out_dir = os.path.join(base_path, country)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{date_label}.parquet")
    df.to_parquet(out_path, engine="pyarrow", index=False)
    logger.info("Guardado local parquet: %s", out_path)
    return out_path

def upload_to_s3(file_path: str, bucket: str, s3_key: str, aws_region=None):
    """
    Sube un archivo local a un bucket de AWS S3.

    Args:
        file_path (str): Ruta local del archivo a subir.
        bucket (str): Nombre del bucket de S3.
        s3_key (str): Ruta/clave en el bucket donde se almacenará el archivo.
        aws_region (str, opcional): Región de AWS. Si no se proporciona, se usa la región por defecto.

    Comportamiento:
        - Utiliza boto3 para conectarse a S3 y subir el archivo.
        - Registra información en el logger sobre la operación.
        - Si ocurre un error, lo registra y lanza la excepción.
    
    Ejemplo:
         upload_to_s3("data/MX/2023-09-01.parquet", "my-bucket", "mx/2023-09-01.parquet")
    """
    s3 = boto3.client('s3', region_name=aws_region) if aws_region else boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket, s3_key)
        logger.info("Archivo %s subido a s3://%s/%s", file_path, bucket, s3_key)
    except Exception:
        logger.exception("Error subiendo %s a S3", file_path)
        raise
