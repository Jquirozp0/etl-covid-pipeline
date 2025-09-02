"""
Archivo de configuración del proyecto.

Este módulo carga variables de entorno desde un archivo `.env` y define constantes
utilizadas en todo el proyecto, como URLs de API, credenciales de AWS, fechas de consulta
y países a analizar.

Variables principales:
- BASE_API_URL: URL base de la API de COVID.
- AWS_ACCESS_KEY_ID: ID de la clave de acceso a AWS.
- AWS_SECRET_ACCESS_KEY: Clave secreta de AWS.
- AWS_DEFAULT_REGION: Región por defecto de AWS.
- S3_BUCKET_NAME: Nombre del bucket de S3 donde se almacenan los datos.
- COVID_DATE: Fecha para consultar datos COVID (YYYY-MM-DD).
- COUNTRIES: Lista de países en formato ISO-3166-1 alpha-2.
- RISK_THRESHOLDS: Diccionario con los umbrales de riesgo según casos reportados.
"""
from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# URL base de la API de COVID
BASE_API_URL = os.getenv("BASE_API_URL", "https://covid-api.com/api")
# Credenciales y configuración de AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
# Fecha de consulta de datos COVID (formato YYYY-MM-DD)
COVID_DATE = os.getenv("COVID_DATE", "2023-09-01")
# Lista de países a consultar (ISO-3166-1 alpha-2)
COUNTRIES = os.getenv("COUNTRIES", "MX,CO,PE").split(",")
# Umbrales de riesgo según número de casos
RISK_THRESHOLDS = {
    "low": 100,
    "medium": 500,
    "high": 1000
}
