import os
import shutil
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from utils import log

# ----------------------------------------------------
# Rutas de las tablas Delta Lake
# ----------------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(script_dir)

flights_raw_path = os.path.join(base_path, "data_lake", "flights", "processed")
airports_raw_path = os.path.join(base_path, "data_lake", "airports", "processed")
processed_enriched_path = os.path.join(base_path, "data_lake", "flights", "processed_enriched_pandas")
agg_path = os.path.join(base_path, "data_lake", "flights", "agg_by_status")

def save_delta_table_robust(
    df_pandas: pd.DataFrame,
    path: str,
    mode: str = "append",
    partition_cols: list | None = None,
    hard_overwrite: bool = False,
):
    """
    Guarda un DataFrame en Delta Lake, rellenando valores nulos para evitar errores.
    """
    if df_pandas is None or df_pandas.empty:
        log("⚠️ DataFrame vacío; no se escribe nada.")
        return

    for col in df_pandas.columns:
        dtype = df_pandas[col].dtype
        if pd.api.types.is_numeric_dtype(dtype):
            df_pandas.loc[:, col] = df_pandas[col].astype('float64').fillna(0)
        elif pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            df_pandas.loc[:, col] = df_pandas[col].astype('string').fillna('')
        elif pd.api.types.is_bool_dtype(dtype):
            df_pandas.loc[:, col] = df_pandas[col].astype('boolean').fillna(False)
        else:
            df_pandas.loc[:, col] = df_pandas[col].fillna('')
    
    if partition_cols:
        for col in partition_cols:
            if col not in df_pandas.columns:
                df_pandas[col] = "unknown"
            df_pandas.loc[:, col] = df_pandas[col].fillna("unknown").astype("string")

    if hard_overwrite and os.path.exists(path):
        try:
            shutil.rmtree(path)
            log(f"🧹 Eliminada tabla Delta existente en {path} (hard overwrite).")
        except Exception as e:
            log(f"⚠️ No se pudo eliminar {path}: {e}")

    write_deltalake(
        path,
        df_pandas,
        mode=("overwrite" if hard_overwrite else mode),
        partition_by=partition_cols,
    )
    log(f"✅ Guardado {len(df_pandas)} filas en {path} (modo={'overwrite' if hard_overwrite else mode}, partition_by={partition_cols})")

def load_data():
    """Carga tablas Delta Lake como DataFrames de Pandas."""
    try:
        log("🔄 Cargando tablas Delta...")
        df_flights = DeltaTable(flights_raw_path).to_pandas()
        df_airports = DeltaTable(airports_raw_path).to_pandas()
        log(f"✅ Cargados {len(df_flights)} vuelos y {len(df_airports)} aeropuertos.")
        return df_flights, df_airports
    except TableNotFoundError as e:
        log(f"❌ No se pudieron cargar tablas Delta: {e}")
        raise SystemExit(1)

def clean_and_normalize(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Aplana una columna JSON anidada y la une al DataFrame principal."""
    if col in df.columns:
        normalized_df = pd.json_normalize(df[col]).add_prefix(f"{col}_")
        return df.drop(col, axis=1).merge(normalized_df, left_index=True, right_index=True)
    return df

def clean_and_enrich_data(df_flights: pd.DataFrame, df_airports: pd.DataFrame) -> pd.DataFrame:
    """Realiza la limpieza y el enriquecimiento de los datos de vuelos."""
    log("🧹 Limpiando y aplanando DataFrame de vuelos...")
    
    # 1. Aplanar las columnas anidadas
    for col in ["departure", "arrival", "airline", "flight", "aircraft", "live"]:
        df_flights = clean_and_normalize(df_flights, col)

    # 2. Renombrar columnas para unirse correctamente
    df_flights.rename(columns={
        'departure_iata': 'departure_iata_code',
        'arrival_iata': 'arrival_iata_code',
        'airline_iata': 'airline_iata_code',
        'aircraft_icao': 'aircraft_icao_code',
        'flight_date': 'flight_date',
        'flight_status': 'status'
    }, inplace=True)
    
    # 3. Asegurar que las columnas de unión existan antes del merge (corrección clave)
    required_cols = ['departure_iata_code', 'arrival_iata_code']
    for col in required_cols:
        if col not in df_flights.columns:
            log(f"⚠️ Columna '{col}' no encontrada en el DataFrame de vuelos; se creará con valores nulos.")
            df_flights[col] = pd.NA

    log("🤝 Enriqueciendo datos...")
    
    # 4. Unir con la tabla de aeropuertos de salida
    df_airports_dep = df_airports.rename(columns={
        "iata_code": "departure_iata_code",
        "airport_name": "departure_airport_name"
    })
    df_enriched = pd.merge(df_flights, df_airports_dep, on="departure_iata_code", how="left")

    # 5. Unir con la tabla de aeropuertos de llegada
    df_airports_arr = df_airports.rename(columns={
        "iata_code": "arrival_iata_code",
        "airport_name": "arrival_airport_name"
    })
    df_enriched = pd.merge(df_enriched, df_airports_arr, on="arrival_iata_code", how="left")

    # 6. Crear la columna de retraso
    if 'arrival_delay' in df_enriched.columns:
        df_enriched['is_delayed'] = pd.to_numeric(df_enriched['arrival_delay'], errors='coerce').fillna(0) > 0
    else:
        df_enriched['is_delayed'] = False
    
    log("✅ Limpieza y enriquecimiento completado.")
    return df_enriched

def aggregate_flights(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega datos por estado del vuelo."""
    log("📊 Agregando datos por estado...")
    if 'status' in df.columns:
        return df.groupby('status').size().reset_index(name='count')
    return pd.DataFrame()

if __name__ == "__main__":
    df_flights_raw, df_airports_raw = load_data()
    df_enriched = clean_and_enrich_data(df_flights_raw, df_airports_raw)

    final_cols = [
        "flight_date", "status", "is_delayed", "departure_iata_code", 
        "departure_airport_name", "arrival_iata_code", "arrival_airport_name"
    ]
    
    for col in final_cols:
        if col not in df_enriched.columns:
            df_enriched[col] = pd.NA
    
    df_final = df_enriched[final_cols]
    
    df_final.loc[:, 'flight_date'] = df_final['flight_date'].astype('string').fillna('unknown')

    if not df_final.empty:
        save_delta_table_robust(
            df_final,
            processed_enriched_path,
            partition_cols=["flight_date"],
            hard_overwrite=True
        )
        log(f"✅ Guardado DataFrame final en {processed_enriched_path}")
    else:
        log("⚠️ El DataFrame final está vacío, no se escribe la tabla enriquecida.")

    df_agg = aggregate_flights(df_final)
    if not df_agg.empty:
        save_delta_table_robust(
            df_agg,
            agg_path,
            hard_overwrite=True
        )
        log(f"✅ Guardado DataFrame de agregación en {agg_path}")