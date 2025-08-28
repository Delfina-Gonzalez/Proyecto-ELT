from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path
from src.config import get_partition_path

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("Proyecto ELT SpaceX") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def save_to_delta(df: pd.DataFrame, endpoint_name: str, incremental: bool = True, mode: str = "append"):
    path = get_partition_path(endpoint_name, incremental)
    Path(path).mkdir(parents=True, exist_ok=True)
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode(mode).save(path)