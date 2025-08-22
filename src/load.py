import pandas as pd
import os
from utils import log, ensure_dir

# ----------------------------
# Función para guardar DataFrame en "data lake" con particiones
# ----------------------------
def save_parquet(df: pd.DataFrame, path: str, partition_col=None):
    """
    Guarda un DataFrame en parquet. Si partition_col está definida,
    crea subcarpetas por valor de la columna.
    """
    ensure_dir(path)
    
    if partition_col and partition_col in df.columns:
        for partition_value, sub_df in df.groupby(partition_col):
            partition_path = os.path.join(path, f"{partition_col}={partition_value}")
            ensure_dir(partition_path)
            sub_df.to_parquet(os.path.join(partition_path, "data.parquet"), index=False, engine="pyarrow")
            log(f"Guardado {len(sub_df)} filas en {partition_path}")
    else:
        df.to_parquet(os.path.join(path, "data.parquet"), index=False, engine="pyarrow")
        log(f"Guardado {len(df)} filas en {path}")

# ----------------------------
# Cargar DataFrames crudos
# ----------------------------
flights_path = "data_lake/flights/flights_raw.parquet"
airports_path = "data_lake/airports/airports_raw.parquet"

df_flights = pd.read_parquet(flights_path, engine="pyarrow")
df_airports = pd.read_parquet(airports_path, engine="pyarrow")

log(f"✅ DataFrames cargados: flights {len(df_flights)} filas, airports {len(df_airports)} filas")

# ----------------------------
# Guardar procesados
# ----------------------------
save_parquet(df_flights, "data_lake/flights/processed", partition_col="flight_date")
save_parquet(df_airports, "data_lake/airports/processed")  # sin particiones