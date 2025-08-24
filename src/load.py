import os
import shutil
import pandas as pd
from deltalake import write_deltalake
from utils import log, ensure_dir

def save_delta_table(
    df_pandas: pd.DataFrame,
    path: str,
    mode: str = "append",
    partition_cols: list | None = None,
    hard_overwrite: bool = False,
):
    """
    Guarda un DataFrame de Pandas en Delta Lake, rellenando valores nulos para evitar errores de esquema.
    """
    if df_pandas is None or df_pandas.empty:
        log("⚠️ DataFrame vacío; no se escribe nada.")
        return

    # CORRECCIÓN CLAVE: Convertir las columnas a tipos que puedan manejar nulos.
    for col in df_pandas.columns:
        if pd.api.types.is_numeric_dtype(df_pandas[col]):
            # Para tipos numéricos, convertimos a float y rellenamos nulos con 0
            df_pandas[col] = df_pandas[col].astype('float64').fillna(0)
        elif pd.api.types.is_object_dtype(df_pandas[col]):
            # Para objetos (incluyendo diccionarios anidados), los convertimos a string y rellenamos nulos con cadena vacía.
            df_pandas[col] = df_pandas[col].astype('string').fillna('')
        elif pd.api.types.is_bool_dtype(df_pandas[col]):
            df_pandas[col] = df_pandas[col].astype('boolean').fillna(False)
        else:
            # Para otros tipos, rellenamos con un valor predeterminado.
            df_pandas[col] = df_pandas[col].fillna('')
    
    if partition_cols:
        for col in partition_cols:
            if col not in df_pandas.columns:
                raise ValueError(f"❌ Columna de partición '{col}' no existe en el DataFrame.")
            df_pandas[col] = df_pandas[col].fillna("unknown").astype("string")

    if hard_overwrite and os.path.exists(path):
        try:
            shutil.rmtree(path)
            log(f"🧹 Eliminada tabla Delta existente en {path} (hard overwrite).")
        except Exception as e:
            log(f"⚠️ No se pudo eliminar {path}: {e}")

    ensure_dir(path)

    write_deltalake(
        path,
        df_pandas,
        mode=("overwrite" if hard_overwrite else mode),
        partition_by=partition_cols,
    )
    log(f"✅ Guardado {len(df_pandas)} filas en {path} (modo={'overwrite' if hard_overwrite else mode}, partition_by={partition_cols})")

# ----------------------------
# Cargar DataFrames crudos (Parquet)
# ----------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(script_dir)

flights_path_raw = os.path.join(base_path, "data_lake", "flights", "flights_raw.parquet")
airports_path_raw = os.path.join(base_path, "data_lake", "airports", "airports_raw.parquet")

if not (os.path.exists(flights_path_raw) and os.path.exists(airports_path_raw)):
    log("❌ No se encontraron los archivos Parquet crudos. Asegúrate de que `extract.py` se ejecutó correctamente y generó los archivos.")
    raise SystemExit(1)

df_flights_raw = pd.read_parquet(flights_path_raw, engine="pyarrow")
df_airports_raw = pd.read_parquet(airports_path_raw, engine="pyarrow")
log(f"✅ Crudos cargados: flights={len(df_flights_raw)} filas, airports={len(df_airports_raw)} filas")

# ----------------------------
# Guardar en Delta Lake (hard overwrite para evitar conflictos de schema)
# ----------------------------
processed_path_flights = os.path.join(base_path, "data_lake", "flights", "processed")
processed_path_airports = os.path.join(base_path, "data_lake", "airports", "processed")

if "flight_date" not in df_flights_raw.columns:
    log("⚠️ 'flight_date' no existe en flights_raw; se crea como 'unknown' para particionado.")
    df_flights_raw["flight_date"] = "unknown"

save_delta_table(
    df_flights_raw,
    processed_path_flights,
    partition_cols=["flight_date"],
    hard_overwrite=True,
)

save_delta_table(
    df_airports_raw,
    processed_path_airports,
    mode="overwrite",
    hard_overwrite=True,
)
log("✅ Datos cargados en Delta Lake.")