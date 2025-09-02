"""
Módulo de utilidades para el pipeline ETL.

Funciones:
- setup_logging: Configura logging con archivo y consola.
- requests_session_with_retries: Crea una sesión HTTP con reintentos automáticos.
"""
import logging
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

def setup_logging(log_path="logs/etl.log"):
    """
    Configura el logger para el pipeline ETL.

    Args:
        log_path (str, opcional): Ruta del archivo de log. Por defecto 'logs/etl.log'.

    Returns:
        logging.Logger: Logger configurado para ser usado en el proyecto.

    Comportamiento:
        - Crea la carpeta del log si no existe.
        - Configura logging a nivel INFO.
        - Envía logs tanto a archivo como a la consola.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("etl")

def requests_session_with_retries(retries=3, backoff_factor=0.3, status_forcelist=(500,502,504)):
    """
    Crea una sesión HTTP con reintentos automáticos para solicitudes robustas.

    Args:
        retries (int, opcional): Número total de reintentos. Por defecto 3.
        backoff_factor (float, opcional): Factor de retroceso exponencial entre intentos. Por defecto 0.3.
        status_forcelist (tuple, opcional): Códigos de estado HTTP que disparan un reintento. Por defecto (500,502,504).

    Returns:
        requests.Session: Sesión de requests configurada con reintentos.

    Comportamiento:
        - Permite reintentos automáticos en caso de errores transitorios.
        - Aplica el backoff_factor para espaciar los reintentos.
        - Monta adaptadores en los esquemas http y https.

    Ejemplo:
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET","POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
