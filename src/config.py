import os
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
    if incremental:
        now = datetime.utcnow()
        path = DATA_RAW / endpoint_name / f"year={now.year}" / f"month={now.month}" / f"day={now.day}"
        return str(path)
    
    path = DATA_RAW / endpoint_name
    return str(path)