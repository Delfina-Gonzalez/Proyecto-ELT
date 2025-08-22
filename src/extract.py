import requests
import pandas as pd
import configparser
from utils import log, extract_datetime_from_dict, ensure_dir

# ----------------------------
# Leer API Key desde pipeline.config
# ----------------------------
config = configparser.ConfigParser()
config.read("pipeline.config")
API_KEY = config["DEFAULT"]["AVIATIONSTACK_API_KEY"]

ensure_dir("data_lake/flights")
ensure_dir("data_lake/airports")

# ----------------------------
# Endpoint temporal: vuelos
# ----------------------------
url_flights = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}"
try:
    response_flights = requests.get(url_flights)
    response_flights.raise_for_status()
    data_flights = response_flights.json()
    df_flights = pd.DataFrame(data_flights.get("data", []))
    
    # Extraer fecha de salida
    if "departure" in df_flights.columns:
        df_flights["flight_date"] = df_flights["departure"].apply(lambda x: extract_datetime_from_dict(x))

    log(f"✅ DataFrame flights válido: {len(df_flights)} registros, columnas: {list(df_flights.columns)}")
    
    # Guardar crudo
    df_flights.to_parquet("data_lake/flights/flights_raw.parquet", engine="pyarrow", index=False)

except Exception as e:
    log(f"[ERROR] Al extraer vuelos: {e}")

# ----------------------------
# Endpoint estático: aeropuertos
# ----------------------------
url_airports = f"http://api.aviationstack.com/v1/airports?access_key={API_KEY}"
try:
    response_airports = requests.get(url_airports)
    response_airports.raise_for_status()
    data_airports = response_airports.json()
    df_airports = pd.DataFrame(data_airports.get("data", []))

    log(f"✅ DataFrame airports válido: {len(df_airports)} registros, columnas: {list(df_airports.columns)}")
    
    # Guardar crudo
    df_airports.to_parquet("data_lake/airports/airports_raw.parquet", engine="pyarrow", index=False)

except Exception as e:
    log(f"[ERROR] Al extraer aeropuertos: {e}")