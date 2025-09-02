"""
Módulo de transformación de datos COVID.

Funciones para:
- Normalizar nombres de columnas a snake_case.
- Filtrar y procesar datos por país.
- Calcular métricas como casos nuevos, tasa de crecimiento y clasificación de riesgo.

"""
from datetime import datetime, timedelta
import pandas as pd
import re
import logging

logger = logging.getLogger("etl.transform")

def to_snake_case(s: str) -> str:
    """
    Convierte un string a formato snake_case.

    Args:
        s (str): Texto original.

    Returns:
        str: Texto en snake_case, sin caracteres especiales.
    
    """
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    s = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s)
    s = s.replace(" ", "_")
    s = re.sub(r'[^0-9a-zA-Z_]+', "", s)
    return s.lower()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza todas las columnas de un DataFrame a snake_case.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame con nombres de columnas normalizados.
    """
    df = df.copy()
    df.columns = [to_snake_case(c) for c in df.columns]
    return df

def process_country_df(df: pd.DataFrame, country: str, risk_thresholds: dict):
    """
    Procesa un DataFrame por país, calculando métricas y clasificando riesgo.

    Transformaciones:
    - Normaliza columnas a snake_case.
    - Filtra los últimos 30 días.
    - Asegura tipos de datos correctos.
    - Calcula casos nuevos y tasa de crecimiento.
    - Clasifica el riesgo según umbrales proporcionados.
    - Agrega columna 'country'.

    Args:
        df (pd.DataFrame): DataFrame crudo de la API.
        country (str): Código ISO del país.
        risk_thresholds (dict): Umbrales para clasificar riesgo ('low', 'medium', 'high').

    Returns:
        tuple: (df_transformado, totales)
            - df_transformado (pd.DataFrame): DataFrame procesado listo para carga.
            - totales (dict): Diccionario con totales de confirmados y casos nuevos.
    """
    if df.empty:
        logger.warning("DataFrame vacío para %s, no se puede transformar", country)
        return df, {"country": country, "total_confirmed": 0, "total_new_cases": 0}
    # Normalizar nombres de columnas
    df = normalize_columns(df)
    # Detectar columna de fecha
    date_col = next((c for c in df.columns if "date" in c), df.columns[0])
    df[date_col] = pd.to_datetime(df[date_col])

    # Filtrar últimos 30 días
    today = datetime.today()
    last_30_days = today - timedelta(days=30)
    df = df[(df[date_col] >= last_30_days) & (df[date_col] <= today)]
    df[date_col] = pd.to_datetime(df[date_col])

    # detectar columna de confirmados
    if "confirmed" not in df.columns and "cases" in df.columns:
        df = df.rename(columns={"cases": "confirmed"})
    if "confirmed" not in df.columns:
        df["confirmed"] = 0

    df["confirmed"] = df["confirmed"].fillna(0).astype(int)
    df = df.sort_values(date_col)

    # Calcular casos nuevos y casos previos
    df["new_cases"] = df["confirmed"].diff().fillna(df["confirmed"]).astype(int)
    df["prev_confirmed"] = df["confirmed"].shift(1).fillna(0).astype(int)

    # Calcular tasa de crecimiento de forma segura
    def safe_growth(new_cases, prev):
        return (new_cases / prev) if prev > 0 else 0.0

    df["growth_rate"] = df.apply(lambda r: safe_growth(r["new_cases"], r["prev_confirmed"]), axis=1)
    # Clasificar riesgo según casos nuevos y umbrales
    def classify(n):
        if n > risk_thresholds.get("medium", 10000):
            return "alto"
        if n > risk_thresholds.get("low", 1000):
            return "medio"
        return "bajo"

    df["risk"] = df["new_cases"].apply(classify)
    df["country"] = country
    # Totales por país
    totals = {
        "country": country,
        "total_confirmed": int(df["confirmed"].max()) if not df.empty else 0,
        "total_new_cases": int(df["new_cases"].sum()) if not df.empty else 0
    }

    return df, totals
