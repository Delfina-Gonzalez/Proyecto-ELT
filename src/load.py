import os
import shutil
import pandas as pd
from deltalake import write_deltalake
from utils import log, ensure_dir

# ----------------------------
# Función para guardar DataFrame en Delta Lake (con hard overwrite opcional)
# ----------------------------
def save_delta_table(
    df_pandas: pd.DataFrame,
    path: str,
    mode: str = "append",
    partition_cols: list | None = None,
    hard_overwrite: bool = False,
):
    """
    Guarda un DataFrame de Pandas en Delta Lake.

    :param df_pandas: DataFrame de Pandas a guardar.
    :param path: Ruta destino (carpeta de la tabla Delta).
    :param mode: 'append' o 'overwrite'.
    :param partition_cols: Columnas de particionado.
    :param hard_overwrite: Si True, borra físicamente la carpeta antes de escribir.
    """
    if df_pandas is None or df_pandas.empty:
        log("⚠️ DataFrame vacío; no se escribe nada.")
        return

    if partition_cols:
        for col in partition_cols:
            if col not in df_pandas.columns:
                raise ValueError(f"❌ Columna de partición '{col}' no existe en el DataFrame.")

    # Hard overwrite: elimina completamente la tabla para evitar conflictos de schema
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
flights_path_raw = "../data_lake/flights/flights_raw.parquet"
airports_path_raw = "../data_lake/airports/airports_raw.parquet"

if not (os.path.exists(flights_path_raw) and os.path.exists(airports_path_raw)):
    log("❌ No se encontraron los parquet crudos. Ejecutá primero src/extract.py.")
    raise SystemExit(1)

df_flights_raw = pd.read_parquet(flights_path_raw, engine="pyarrow")
df_airports_raw = pd.read_parquet(airports_path_raw, engine="pyarrow")
log(f"✅ Crudos cargados: flights={len(df_flights_raw)} filas, airports={len(df_airports_raw)} filas")

# ----------------------------
# Guardar en Delta Lake (hard overwrite para evitar conflictos de schema)
# ----------------------------
processed_path_flights = "../data_lake/flights/processed"
processed_path_airports = "../data_lake/airports/processed"

# Vuelos (dinámicos): particionar por flight_date y hard overwrite para esquema estable
if "flight_date" not in df_flights_raw.columns:
    log("⚠️ 'flight_date' no existe en flights_raw; se crea como 'unknown' para particionado.")
    df_flights_raw["flight_date"] = "unknown"

save_delta_table(
    df_flights_raw,
    processed_path_flights,
    partition_cols=["flight_date"],
    hard_overwrite=True,  # clave para evitar el error de schema
)

# Aeropuertos (estáticos): full refresh
save_delta_table(
    df_airports_raw,
    processed_path_airports,
    mode="overwrite",
    hard_overwrite=True,  # mantenemos esquema limpio siempre
)
log("✅ Datos cargados en Delta Lake.")
