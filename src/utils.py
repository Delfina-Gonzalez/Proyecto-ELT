from datetime import datetime
import os
import logging
import pandas as pd
from deltalake import write_deltalake
from typing import Optional, List
from pathlib import Path
import shutil

# Configuración del logger
def setup_logger():
    """Configura y retorna un logger para el proyecto."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    return logging.getLogger(__name__)

log = setup_logger().info

# Funciones de ayuda
def ensure_dir(file_path: str):
    """
    Asegura que el directorio de un archivo exista.

    Args:
        file_path: Ruta completa del archivo.
    """
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_data_to_parquet(df: pd.DataFrame, folder_path: str, file_name: str):
    """
    Guarda un DataFrame en formato Parquet.

    Args:
        df: DataFrame de Pandas a guardar.
        folder_path: Ruta de la carpeta donde se guardará el archivo.
        file_name: Nombre del archivo sin extensión.
    """
    file_path = os.path.join(folder_path, f"{file_name}.parquet")
    ensure_dir(file_path)
    try:
        df.to_parquet(file_path, engine="pyarrow", index=False)
        log(f"✅ Guardado DataFrame en {file_path}")
    except Exception as e:
        log(f"❌ Error al guardar en formato Parquet: {e}")

def save_df_as_delta(
    df_pandas: pd.DataFrame,
    path: Path,
    partition_cols: Optional[List[str]] = None,
    mode: str = "overwrite",
    hard_overwrite: bool = False
):
    """
    Guarda un DataFrame de Pandas como una tabla Delta.

    Args:
        df_pandas: DataFrame a guardar.
        path: Ruta donde se guardará la tabla Delta.
        partition_cols: Lista de columnas para particionar la tabla.
        mode: Estrategia de escritura ('overwrite', 'append', 'ignore').
        hard_overwrite: Si es True, borra completamente la tabla Delta existente antes de escribir.
    """
    try:
        if hard_overwrite and path.exists():
            shutil.rmtree(path)
            log(f"🧹 Eliminada tabla Delta existente en {path} (hard overwrite).")

        # Llenar nulos para evitar problemas de esquema
        for col in df_pandas.columns:
            dtype = df_pandas[col].dtype
            if pd.api.types.is_numeric_dtype(dtype):
                df_pandas[col] = df_pandas[col].astype('float64').fillna(0)
            elif pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
                df_pandas[col] = df_pandas[col].astype('string').fillna('')
            elif pd.api.types.is_bool_dtype(dtype):
                df_pandas[col] = df_pandas[col].astype('boolean').fillna(False)
            else:
                df_pandas[col] = df_pandas[col].fillna('')

        # Asegurar que las columnas de partición existan y tengan el tipo correcto
        if partition_cols:
            for col in partition_cols:
                if col not in df_pandas.columns:
                    df_pandas[col] = "unknown"
                df_pandas[col] = df_pandas[col].fillna("unknown").astype("string")

        write_deltalake(
            table_path=str(path),
            data=df_pandas,
            partition_by=partition_cols,
            mode=mode
        )
        log(f"✅ DataFrame guardado con éxito en: {path} con modo '{mode}'.")
    except Exception as e:
        log(f"❌ Error al guardar DataFrame en Delta Lake: {e}")
    