import os
from datetime import datetime

def ensure_dir(path: str):
    """Crea el directorio si no existe"""
    if not os.path.exists(path):
        os.makedirs(path)

def log(message: str):
    """Imprime mensaje con timestamp"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")

def extract_datetime_from_dict(d: dict, key="scheduled"):
    """
    Extrae la fecha (YYYY-MM-DD) de un dict como 'departure' o 'arrival' de la API.
    Devuelve None si no existe la clave o el valor es None.
    """
    if isinstance(d, dict):
        value = d.get(key)
        if value:
            try:
                return value[:10]  # corta para quedarnos con YYYY-MM-DD
            except Exception:
                return None
    return None