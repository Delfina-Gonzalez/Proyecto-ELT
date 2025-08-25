import os
import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from utils import log, save_df_as_delta
from typing import List
from pathlib import Path
import configparser
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# ----------------------------
# Configuración
# ----------------------------
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
base_path = script_dir.parent
config_path = base_path / "pipeline.config"

config = configparser.ConfigParser()
config.read(config_path)

DATA_LAKE_PATH = base_path / config.get("paths", "data_lake", fallback="data_lake")

# Rutas de las tablas Delta
FLIGHTS_DELTA_PATH = DATA_LAKE_PATH / "flights" / "delta"
AIRPORTS_DELTA_PATH = DATA_LAKE_PATH / "airports" / "delta"
PROCESSED_ENRICHED_PATH = DATA_LAKE_PATH / "flights" / "processed_enriched"
AGG_PATH = DATA_LAKE_PATH / "flights" / "agg_by_status"

def _read_delta_table(path: Path) -> pd.DataFrame:
    """
    Lee una tabla Delta de forma segura y retorna un DataFrame.
    """
    if not path.exists():
        raise TableNotFoundError(f"No existe la ruta Delta: {path}")
    dt = DeltaTable(str(path))
    return dt.to_pandas()

def load_data() -> (pd.DataFrame, pd.DataFrame):
    """
    Carga los DataFrames crudos de las tablas Delta.
    """
    df_flights = pd.DataFrame()
    df_airports = pd.DataFrame()
    try:
        df_flights = _read_delta_table(FLIGHTS_DELTA_PATH)
        log(f"✅ Tabla Delta de vuelos cargada: {len(df_flights)} filas.")
        df_airports = _read_delta_table(AIRPORTS_DELTA_PATH)
        log(f"✅ Tabla Delta de aeropuertos cargada: {len(df_airports)} filas.")
    except TableNotFoundError as e:
        log(f"❌ Error: {e}")
    except Exception as e:
        log(f"❌ Ocurrió un error inesperado al cargar las tablas: {e}")
    return df_flights, df_airports

def clean_and_enrich_data(df_flights_raw: pd.DataFrame, df_airports_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia, aplanada y enriquece los DataFrames de vuelos y aeropuertos.
    """
    log("🧹 Limpiando y aplanando DataFrame de vuelos...")
    df_flights_raw = df_flights_raw.fillna('')

    log("🤝 Enriqueciendo datos...")
    df_airports_raw = df_airports_raw.fillna('')

    # Enriquecer datos de vuelos con información de aeropuertos de salida
    df_enriched = pd.merge(
        df_flights_raw,
        df_airports_raw[['iata_code', 'airport_name']],
        left_on='departure_iata_code',
        right_on='iata_code',
        how='left'
    )
    df_enriched = df_enriched.rename(columns={'airport_name': 'departure_airport_name'}).drop(columns=['iata_code'])
    
    # Enriquecer con información de aeropuertos de llegada
    df_enriched = pd.merge(
        df_enriched,
        df_airports_raw[['iata_code', 'airport_name']],
        left_on='arrival_iata_code',
        right_on='iata_code',
        how='left'
    )
    df_enriched = df_enriched.rename(columns={'airport_name': 'arrival_airport_name'}).drop(columns=['iata_code'])
    
    return df_enriched

def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos para obtener el total de vuelos por estado.
    """
    log("📊 Agregando datos...")
    return df.groupby('status').size().reset_index(name='total_flights')

def main():
    log("--- 🔄 Iniciando la etapa de Transformación ---")
    df_flights_raw, df_airports_raw = load_data()

    if df_flights_raw.empty or df_airports_raw.empty:
        log("⚠️ Uno o ambos DataFrames están vacíos. No se puede continuar con la transformación y el enriquecimiento.")
    else:
        df_enriched = clean_and_enrich_data(df_flights_raw, df_airports_raw)

        final_cols: List[str] = [
            "flight_date", "status", "is_delayed", "departure_iata_code", 
            "departure_airport_name", "arrival_iata_code", "arrival_airport_name"
        ]

        for col in final_cols:
            if col not in df_enriched.columns:
                df_enriched[col] = pd.NA

        df_final = df_enriched[final_cols]
        df_final['flight_date'] = df_final['flight_date'].astype('string').fillna('unknown')

        if not df_final.empty:
            save_df_as_delta(
                df_final,
                PROCESSED_ENRICHED_PATH,
                partition_cols=["flight_date"],
                hard_overwrite=True
            )
            log(f"✅ Guardado DataFrame final en {PROCESSED_ENRICHED_PATH}")
        else:
            log("⚠️ El DataFrame final está vacío, no se escribe la tabla enriquecida.")

        df_agg = aggregate_data(df_final)

        if not df_agg.empty:
            save_df_as_delta(df_agg, AGG_PATH)
            log(f"✅ Guardado DataFrame agregado por estado en {AGG_PATH}")
        else:
            log("⚠️ El DataFrame agregado está vacío, no se escribe la tabla agregada.")

if __name__ == "__main__":
    main()