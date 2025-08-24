import os
import time
import requests
import pandas as pd
from utils import log, ensure_dir
from typing import Optional, Dict
from configparser import ConfigParser # <-- CORRECCIÓN: Se debe importar la clase

# Rutas y configuración
script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(script_dir)
config_path = os.path.join(base_path, "pipeline.config")

# Leer la configuración
config = ConfigParser()
config.read(config_path)

if 'aviationstack' in config and 'api_key' in config['aviationstack']:
    API_KEY = config['aviationstack']['api_key']
else:
    log("❌ La clave 'api_key' no se encontró en la sección [aviationstack] del archivo pipeline.config.")
    API_KEY = None
    
BASE_URL = "http://api.aviationstack.com/v1"

def extract_data(endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> pd.DataFrame:
    """Extrae datos de la API de AviationStack."""
    if not API_KEY:
        log("⚠️ No se puede extraer datos de la API sin una clave. Devolviendo DataFrame vacío.")
        return pd.DataFrame()

    url = f"{BASE_URL}/{endpoint}"
    params = params or {}
    params["access_key"] = API_KEY

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json().get("data", [])
                if not data:
                    log(f"⚠️ {endpoint}: sin datos en la respuesta.")
                return pd.DataFrame(data)

            elif response.status_code == 401:
                log("❌ API Key inválida o no configurada. Revisa AVIATIONSTACK_API_KEY.")
                break

            elif response.status_code == 429:
                log(f"⚠️ Límite de rate en {endpoint}. Reintento {attempt}/{max_retries}...")
                time.sleep(2 * attempt)

            else:
                log(f"❌ Error {response.status_code} en {endpoint}: {response.text}")
                break

        except Exception as e:
            log(f"❌ Excepción en {endpoint}, intento {attempt}: {e}")
            time.sleep(2 * attempt)

    log(f"⚠️ No se pudo extraer datos de {endpoint} tras {max_retries} intentos.")
    return pd.DataFrame()

if __name__ == "__main__":
    log("▶️ Iniciando extracción de datos...")

    # Rutas para guardar los archivos
    flights_path = os.path.join(base_path, "data_lake", "flights", "flights_raw.parquet")
    airports_path = os.path.join(base_path, "data_lake", "airports", "airports_raw.parquet")

    # Extraer y guardar vuelos
    df_flights = extract_data("flights", params={"flight_status": "landed"})
    if df_flights is not None and not df_flights.empty:
        ensure_dir(os.path.dirname(flights_path))
        df_flights.to_parquet(flights_path, index=False)
        log(f"✅ Extracción de vuelos exitosa. {len(df_flights)} filas guardadas.")
    else:
        log("⚠️ No se pudo extraer vuelos. Guardando DataFrame vacío.")
        df_empty = pd.DataFrame(columns=["flight_date", "flight_status", "departure", "arrival", "airline"])
        df_empty.to_parquet(flights_path, index=False)
    
    # Extraer y guardar aeropuertos
    df_airports = extract_data("airports")
    if not df_airports.empty:
        ensure_dir(os.path.dirname(airports_path))
        df_airports.to_parquet(airports_path, index=False)
        log(f"✅ Extracción de aeropuertos exitosa. {len(df_airports)} filas guardadas.")
    else:
        log("⚠️ No se pudo extraer aeropuertos. Guardando DataFrame vacío.")
        df_empty = pd.DataFrame(columns=["airport_name", "iata_code", "icao_code", "city"])
        df_empty.to_parquet(airports_path, index=False)

    log("✅ Proceso de extracción finalizado.")