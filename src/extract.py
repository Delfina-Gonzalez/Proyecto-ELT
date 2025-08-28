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

def fetch_data(endpoint: str) -> pd.DataFrame:
    """
    Transforma la respuesta JSON en DataFrame, con manejo de errores de conexión y formato.
    """
    try:
        data = get_json_from_api(endpoint)
        if isinstance(data, list):
            return pd.json_normalize(data)
        return pd.json_normalize([data])
    except (HTTPError, JSONDecodeError) as e:
        print(f"Error al obtener o procesar datos del endpoint '{endpoint}': {e}")
        return pd.DataFrame() # Devuelve un DataFrame vacío en caso de error