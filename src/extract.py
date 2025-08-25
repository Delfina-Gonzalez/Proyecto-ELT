import os
import requests
import pandas as pd
import configparser
from utils import log, save_data_to_parquet
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime, timedelta

# ----------------------------
# Configuración
# ----------------------------
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
base_path = script_dir.parent
config_path = base_path / "pipeline.config"

config = configparser.ConfigParser()
# Si el archivo de configuración no existe, se crea con secciones mínimas
if not config_path.exists():
    log("⚠️ Archivo 'pipeline.config' no encontrado. Creando uno con valores por defecto.")
    config["aviationstack"] = {"api_key": ""}
    config["urls"] = {"api_url_base": "http://api.aviationstack.com/v1"}
    config["paths"] = {"data_lake": "./data_lake"}
    config["state"] = {"last_flights_run": "2020-01-01"}
    with open(config_path, "w") as f:
        config.write(f)

config.read(config_path)

# Variables globales para la configuración de la API y el Data Lake
API_KEY = os.getenv("AVIATIONSTACK_API_KEY") or config.get("aviationstack", "api_key", fallback=None)
BASE_URL = config.get("urls", "api_url_base", fallback="http://api.aviationstack.com/v1")
DATA_LAKE_PATH = base_path / config.get("paths", "data_lake", fallback="data_lake")

# Rutas de las carpetas
flights_folder = DATA_LAKE_PATH / "flights" / "processed"
airports_folder = DATA_LAKE_PATH / "airports" / "processed"

# ----------------------------
# Funciones de Extracción
# ----------------------------
def extract_data(endpoint: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """Extrae datos de la API de AviationStack."""
    log(f"🔗 Consultando API en el endpoint: {endpoint}")
    params_base = {"access_key": API_KEY, "limit": 100}
    if params:
        params_base.update(params)
    
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params_base)
    response.raise_for_status()
    data = response.json().get("data", [])
    
    if not data:
        log(f"⚠️ El endpoint '{endpoint}' no devolvió datos. Retornando un DataFrame vacío.")
        return pd.DataFrame()

    df = pd.json_normalize(data, sep="_")
    log(f"✅ Datos de '{endpoint}' extraídos: {len(df)} filas.")
    return df

def get_last_run_date() -> str:
    """Lee la fecha de la última ejecución del archivo de configuración."""
    config.read(config_path)
    return config.get("state", "last_flights_run", fallback="2020-01-01")

def update_last_run_date(date: str):
    """Actualiza la fecha de la última ejecución en el archivo de configuración."""
    config.read(config_path)
    config.set("state", "last_flights_run", date)
    with open(config_path, "w") as f:
        config.write(f)
    log(f"🗓️ Fecha de última ejecución actualizada a: {date}")

# ----------------------------------------------------
# Main
# ----------------------------------------------------
def main():
    log("--- 🌍 Iniciando la etapa de Extracción ---")
    
    # ---------------------------------------------------------
    # EXTRACCIÓN INCREMENTAL: vuelos (data más reciente)
    # ---------------------------------------------------------
    last_run_date = get_last_run_date()
    df_flights = pd.DataFrame()
    use_fallback = False
    
    log(f"🔎 Buscando vuelos desde la fecha: {last_run_date}")
    
    try:
        df_flights = extract_data("flights", params={"flight_date": last_run_date})
    except Exception as e:
        log(f"❌ Error al consultar la API de vuelos: {e}")
    
    if df_flights.empty:
        log("⚠️ La API de vuelos no devolvió datos. Usando archivo de prueba...")
        flights_sample_path = flights_folder / "flights_raw_sample.csv"
        if flights_sample_path.exists():
            log(f"✅ Usando el archivo de prueba: {flights_sample_path}")
            df_flights = pd.read_csv(flights_sample_path)
            if 'flight_date' not in df_flights.columns:
                df_flights['flight_date'] = datetime.today().strftime('%Y-%m-%d')
            use_fallback = True
        else:
            log("❌ Fallback de API y archivo de prueba fallidos. No hay datos para procesar.")

    # Renombra las columnas AQUI para estandarizar el esquema
    if not df_flights.empty:
        df_flights = df_flights.rename(columns={
            'dep_iata': 'departure_iata_code',
            'arr_iata': 'arrival_iata_code',
            'flight_status': 'status',
            'is_delayed_bool': 'is_delayed'
        })
        log("✅ Columnas de vuelos renombradas para estandarizar el esquema.")
    
    if not df_flights.empty:
        save_data_to_parquet(df_flights, str(flights_folder), "flights_raw")
        if not use_fallback:
            try:
                next_run_date = (datetime.strptime(last_run_date, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d")
                update_last_run_date(next_run_date)
            except ValueError:
                log("⚠️ No se pudo actualizar la fecha de la última ejecución. Formato incorrecto.")
    else:
        log("⚠️ No se extrajeron nuevos vuelos. No se actualiza 'last_flights_run'.")

    # ---------------------------------------------------------
    # EXTRACCIÓN FULL: aeropuertos (metadatos)
    # ---------------------------------------------------------

    df_airports = pd.DataFrame()
    try:
        df_airports = extract_data("airports")
    except Exception as e:
        log(f"❌ Error al consultar la API de aeropuertos: {e}")

    if df_airports.empty:
        sample_path = airports_folder / "airports_raw_sample.csv"
        if sample_path.exists():
            log(f"⚠️ La API de aeropuertos no devolvió datos. Usando archivo de prueba: {sample_path}")
            df_airports = pd.read_csv(sample_path)
        else:
            log("❌ Fallback de aeropuertos fallido. No hay datos para procesar.")
    
    # Renombra las columnas AQUI para estandarizar el esquema
    if not df_airports.empty:
        df_airports = df_airports.rename(columns={'iata': 'iata_code', 'airport': 'airport_name'})
        log("✅ Columnas de aeropuertos renombradas para estandarizar el esquema.")

    if not df_airports.empty:
        save_data_to_parquet(df_airports, str(airports_folder), "airports_raw")
    else:
        log("⚠️ No se extrajeron nuevos aeropuertos. No se crea el archivo Parquet.")

if __name__ == "__main__":
    main()