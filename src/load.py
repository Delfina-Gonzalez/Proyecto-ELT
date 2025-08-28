from pathlib import Path
from pyspark.sql import SparkSession
import pandas as pd
import json
from src.config import get_partition_path

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("Proyecto ELT SpaceX") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def save_to_delta(df: pd.DataFrame, endpoint_name: str, incremental: bool = True, mode="overwrite"):
    """
    Guarda un DataFrame de Pandas en formato Delta Lake.
    Soporta partición por fecha si incremental=True.
    """
    # 1. No guardar si está vacío
    if df.empty:
        print(f"[WARN] DataFrame vacío para {endpoint_name}, no se guarda nada.")
        return

    # 2. Normalización columnas para Spark
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    # 3. Crear carpeta destino usando partición
    path = get_partition_path(endpoint_name, incremental)
    Path(path).mkdir(parents=True, exist_ok=True)

    # 4. SparkSession
    global spark
    if "spark" not in globals():
        spark = SparkSession.builder.appName("ELT-API").getOrCreate()

    # 5. Pandas -> Spark
    spark_df = spark.createDataFrame(df)

    # 6. Guardar en Delta
    spark_df.write.format("delta").mode(mode).save(path)
    print(f"[INFO] Guardado correcto: {endpoint_name} en {path}")
