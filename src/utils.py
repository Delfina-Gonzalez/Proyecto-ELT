import os
from datetime import datetime


def ensure_dir(path: str):
    """
    Crea el directorio si no existe.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def log(message: str):
    """
    Imprime un mensaje con un timestamp.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")


def extract_datetime_from_dict(d: dict, key: str = "scheduled"):
    """
    Extrae la fecha (YYYY-MM-DD) de un diccionario anidado como 'departure' o 'arrival' de la API.
    Devuelve None si no existe la clave o el valor es None.

    :param d: Diccionario de entrada (ejemplo: departure/arrival de la API).
    :param key: Clave a buscar (por defecto 'scheduled').
    :return: Fecha en formato YYYY-MM-DD o None.
    """
    if isinstance(d, dict):
        value = d.get(key)
        if value:
            try:
                return value[:10]  # cortar para quedarse con YYYY-MM-DD
            except Exception:
                return None
    return None
