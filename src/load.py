import os
import pandas as pd
from utils import log, save_df_as_delta
from pathlib import Path
import configparser

# ----------------------------
# Configuración
# ----------------------------
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
base_path = script_dir.parent
config_path = base_path / "pipeline.config"

config = configparser.ConfigParser()
config.read(config_path)

DATA_LAKE_PATH = base_path / config.get("paths", "data_lake", fallback="data_lake")

# Rutas RAW (Parquet)
FLIGHTS_RAW_PARQUET = DATA_LAKE_PATH / "flights" / "processed" / "flights_raw.parquet"
AIRPORTS_RAW_PARQUET = DATA_LAKE_PATH / "airports" / "processed" / "airports_raw.parquet"

# Rutas DESTINO (Delta)
FLIGHTS_DELTA_PATH = DATA_LAKE_PATH / "flights" / "delta"
AIRPORTS_DELTA_PATH = DATA_LAKE_PATH / "airports" / "delta"

def _load_raw_data(path: Path) -> pd.DataFrame:
    """Carga un archivo Parquet de forma segura, retornando un DataFrame vacío si falla."""
    try:
        if path.exists() and os.path.getsize(path) > 0:
            df = pd.read_parquet(path, engine="pyarrow")
            log(f"✅ Archivo cargado desde: {path} - Filas: {len(df)}")
            return df
        else:
            log(f"⚠️ Archivo no encontrado o vacío: {path}")
            return pd.DataFrame()
    except Exception as e:
        log(f"❌ Error al leer el archivo {path}: {e}")
        return pd.DataFrame()

def main():
    log("--- 📄 Iniciando la etapa de Carga ---")
    
    # Procesar datos de vuelos
    df_flights_raw = _load_raw_data(FLIGHTS_RAW_PARQUET)
    if not df_flights_raw.empty:
        log("✅ Procesando datos de vuelos...")
        save_df_as_delta(
            df_flights_raw,
            FLIGHTS_DELTA_PATH,
            partition_cols=["flight_date"],
            mode="append",
            hard_overwrite=True
        )
        log(f"✅ Tabla Delta de vuelos actualizada en: {FLIGHTS_DELTA_PATH}")
    else:
        log("⚠️ No hay datos de vuelos para cargar. Saltando la carga de vuelos.")

    # Procesar datos de aeropuertos
    df_airports_raw = _load_raw_data(AIRPORTS_RAW_PARQUET)
    if not df_airports_raw.empty:
        log("✅ Procesando datos de aeropuertos...")
        save_df_as_delta(
            df_airports_raw,
            AIRPORTS_DELTA_PATH,
            hard_overwrite=True
        )
        log(f"✅ Tabla Delta de aeropuertos creada/actualizada en: {AIRPORTS_DELTA_PATH}")
    else:
        log("⚠️ No hay datos de aeropuertos para cargar. Saltando la carga de aeropuertos.")

if __name__ == "__main__":
    main()