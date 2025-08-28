from datetime import datetime
from pathlib import Path

try:
    # Esto funciona cuando el script se ejecuta directamente
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    # Esto funciona cuando se ejecuta desde un notebook
    BASE_DIR = Path.cwd().parent

DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"

API_BASE_URL = "https://api.spacexdata.com/v4"

ENDPOINTS = {
    "latest_launch": "/launches/latest",
    "upcoming_launches": "/launches/upcoming",
    "rockets": "/rockets",
    "dragons": "/dragons"
}

def get_partition_path(endpoint_name: str, incremental: bool = True) -> str:
    """
    Genera la ruta del directorio para guardar datos en el data lake.

    Esta función crea una ruta de archivo para almacenar datos en la capa
    'raw', con la opción de particionar los datos por fecha para las
    extracciones incrementales.

    Args:
        endpoint_name (str): El nombre del endpoint de la API, usado como
            nombre del subdirectorio en la capa 'raw'.
        incremental (bool, opcional): Si es True, la ruta incluye
            particiones de año, mes y día. Si es False, la ruta es plana.
            Por defecto es True.

    Returns:
        str: La ruta de archivo completa para guardar los datos.
    """
    if incremental:
        now = datetime.utcnow()
        path = DATA_RAW / endpoint_name / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        return str(path)
    
    path = DATA_RAW / endpoint_name
    return str(path)