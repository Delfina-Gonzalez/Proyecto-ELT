import requests
import pandas as pd
from src.config import API_BASE_URL, ENDPOINTS
from requests.exceptions import HTTPError
from json.decoder import JSONDecodeError

def get_json_from_api(endpoint: str) -> dict:
    """
    Obtiene datos crudos desde un endpoint de la SpaceX API.
    """
    url = f"{API_BASE_URL}{ENDPOINTS[endpoint]}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def convert_column_types(df: pd.DataFrame, type_mappings: dict) -> pd.DataFrame:
    """
    Convierte el tipo de datos de columnas de un DataFrame asegurando compatibilidad
    con Spark (evitando listas, dicts o columnas completamente nulas).

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        type_mappings (dict): Un diccionario donde las claves son los nombres de
            las columnas y los valores son los tipos de datos a los que se
            quieren convertir (ej: 'string', 'int64', 'float64', 'boolean', 'datetime64[ns]').

    Returns:
        pd.DataFrame: El DataFrame con los tipos de datos de las columnas convertidos.
    """

    for col, dtype in type_mappings.items():
        if col not in df.columns:
            print(f"⚠️ Columna {col} no encontrada en DataFrame, se saltea.")
            continue

        try:
            # Si la columna es completamente nula, la eliminamos
            if df[col].isnull().all():
                print(f"⚠️ Columna {col} eliminada porque es completamente nula.")
                df = df.drop(columns=[col])
                continue

            # Si la columna tiene dicts o listas, las convertimos a string
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                print(f"ℹ️ Columna {col} contenía dict/list, se convierte a string.")
                df[col] = df[col].astype(str)

            # Finalmente aplicamos la conversión al tipo deseado
            df[col] = df[col].astype(dtype, errors="raise")

        except Exception as e:
            print(f"❌ No se pudo convertir la columna {col} a {dtype}: {e}")

    return df


def fetch_data(endpoint: str, type_mappings: dict | None = None) -> pd.DataFrame:
    """
    Transforma la respuesta JSON en DataFrame, con manejo de errores de conexión y formato.
    Además asegura compatibilidad de tipos para Spark si se pasa un type_mappings.
    
    Args:
        endpoint (str): URL del endpoint de la API.
        type_mappings (dict | None): Diccionario opcional {columna: tipo}, 
                                     ej. {"id": "int64", "active": "boolean"}.

    Returns:
        pd.DataFrame: DataFrame plano y tipado.
    """
    try:
        data = get_json_from_api(endpoint)

        # Normaliza JSON en DataFrame
        if isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            df = pd.json_normalize([data])

        # Si no vino nada, devolvemos DataFrame vacío
        if df.empty:
            print(f"⚠️ El endpoint '{endpoint}' devolvió datos vacíos.")
            return df

        # Aplica conversiones de tipos si corresponde
        if type_mappings:
            df = convert_column_types(df, type_mappings)

        return df

    except (HTTPError, JSONDecodeError) as e:
        print(f"❌ Error al obtener/procesar datos del endpoint '{endpoint}': {e}")
        return pd.DataFrame()  # Devuelve vacío en caso de error
