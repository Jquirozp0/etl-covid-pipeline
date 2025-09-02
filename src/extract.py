"""
Módulo de extracción de datos COVID.

Este módulo se encarga de obtener datos de COVID-19 desde la API definida en `config.py`.
Incluye funciones para consultar datos por país y fecha, y devuelve los resultados en 
DataFrames de pandas listos para su procesamiento posterior.
"""
import pandas as pd
import logging
from datetime import datetime, timedelta
from config import BASE_API_URL
from utils import requests_session_with_retries

# Configuración del logger para el módulo
logger = logging.getLogger(__name__)

def fetch_country_confirmed(iso_country, date):
    """
    Obtiene los datos de casos confirmados de COVID para un país específico y una fecha.

    Args:
        iso_country (str): Código ISO-3166-1 alpha-2 del país (ej. "MX", "CO").
        date (str): Fecha de consulta en formato "YYYY-MM-DD".

    Returns:
        pd.DataFrame: DataFrame con los datos de la API. Si no hay datos o ocurre un error,
                      devuelve un DataFrame vacío.

    Comportamiento:
        - Realiza la solicitud HTTP con reintentos automáticos para mayor robustez.
        - Convierte los datos JSON recibidos en un DataFrame de pandas.
        - Registra warnings si no hay datos y errores si falla la solicitud.
    
    Ejemplo:
         df = fetch_country_confirmed("MX", "2023-09-01")
         df.head()
    """

    # Crear sesión de requests con reintentos automáticos
    session = requests_session_with_retries()
    # Construir URL y parámetros para la API
    url = f"{BASE_API_URL}/reports"
    params = {"iso": iso_country, "date": date}
              
    try:
        # Realizar la solicitud GET
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        # Convertir datos JSON a DataFrame si hay información
        if "data" in payload and payload["data"]:
            df = pd.json_normalize(payload["data"])
            return df
        else:
            logger.warning("No se encontraron datos para %s en %s", iso_country, date)
            return pd.DataFrame()
    except Exception as e:
        # Registrar cualquier excepción ocurrida
        logger.exception("Error obteniendo datos para %s en %s: %s", iso_country, date, e)
        return pd.DataFrame()

