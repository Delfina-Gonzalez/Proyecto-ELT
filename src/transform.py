import os
import shutil
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from utils import log

flights_raw_path = "../data_lake/flights/processed"
airports_raw_path = "../data_lake/airports/processed"
processed_enriched_path = "../data_lake/flights/processed_enriched_pandas"
agg_path = "../data_lake/flights/agg_by_status"


def load_data():
    """Carga tablas Delta Lake como DataFrames de Pandas."""
    try:
        df_flights = DeltaTable(flights_raw_path).to_pandas()
        df_airports = DeltaTable(airports_raw_path).to_pandas()
        log(f"✅ Cargados {len(df_flights)} vuelos y {len(df_airports)} aeropuertos.")
        return df_flights, df_airports
    except TableNotFoundError as e:
        log(f"❌ No se pudieron cargar tablas Delta: {e}")
        raise SystemExit(1)


def clean_flights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplana JSON (departure, arrival, airline, flight, aircraft, live),
    convierte tipos, maneja nulos y elimina duplicados.
    """
    if df is None or df.empty:
        log("⚠️ df_flights vacío en clean_flights.")
        return pd.DataFrame()

    # Aplanar estructuras anidadas
    df_norm = pd.json_normalize(df.to_dict("records"), sep="_")

    # Convertir delays a numéricos
    for col in ["departure_delay", "arrival_delay"]:
        if col in df_norm.columns:
            df_norm[col] = pd.to_numeric(df_norm[col], errors="coerce").fillna(0)
        else:
            df_norm[col] = 0

    # Rellenar nulos SOLO en columnas object para evitar FutureWarning
    obj_cols = df_norm.select_dtypes(include=["object"]).columns
    df_norm[obj_cols] = df_norm[obj_cols].fillna("N/A")

    # Columnas críticas que el pipeline espera
    required_cols = [
        "departure_iata",
        "arrival_iata",
        "flight_status",
        "flight_date",
        "aircraft_icao",
    ]
    for col in required_cols:
        if col not in df_norm.columns:
            df_norm[col] = "N/A"

    # Eliminar duplicados
    before = len(df_norm)
    df_norm.drop_duplicates(inplace=True)
    log(f"✅ Eliminados {before - len(df_norm)} duplicados.")

    return df_norm


def enrich_flights(df_flights: pd.DataFrame, df_airports: pd.DataFrame) -> pd.DataFrame:
    """
    Une vuelos con aeropuertos (por departure_iata) y crea columnas derivadas.
    """
    if df_flights is None or df_flights.empty:
        log("⚠️ DataFrame de vuelos vacío en enrich_flights.")
        return pd.DataFrame()

    df_enriched = df_flights.copy()

    # Asegurar departure_iata como string
    if "departure_iata" not in df_enriched.columns:
        df_enriched["departure_iata"] = "N/A"
    df_enriched["departure_iata"] = df_enriched["departure_iata"].astype(str)

    # Join con aeropuertos si hay datos
    if df_airports is not None and not df_airports.empty:
        airports = df_airports.copy()
        # Normalizar IATA de aeropuertos
        if "iata_code" not in airports.columns:
            # fallback: algunos dumps usan 'iata'
            if "iata" in airports.columns:
                airports.rename(columns={"iata": "iata_code"}, inplace=True)
            else:
                airports["iata_code"] = "N/A"

        # Tomar una columna de nombre de aeropuerto disponible
        name_col = "airport_name" if "airport_name" in airports.columns else (
            "airport" if "airport" in airports.columns else None
        )
        if name_col is None:
            airports["airport_name"] = airports["iata_code"]
            name_col = "airport_name"

        airports["iata_code"] = airports["iata_code"].astype(str)

        df_enriched = df_enriched.merge(
            airports[["iata_code", name_col]].rename(
                columns={"iata_code": "departure_iata", name_col: "departure_airport_name"}
            ),
            on="departure_iata",
            how="left",
        )
    else:
        df_enriched["departure_airport_name"] = "N/A"

    # Columna derivada: ¿tuvo delay de salida?
    if "departure_delay" in df_enriched.columns:
        df_enriched["is_delayed"] = pd.to_numeric(
            df_enriched["departure_delay"], errors="coerce"
        ).fillna(0) > 0
    else:
        df_enriched["is_delayed"] = False

    # flight_date a datetime (si es parseable)
    if "flight_date" in df_enriched.columns:
        df_enriched["flight_date"] = pd.to_datetime(
            df_enriched["flight_date"], errors="coerce"
        )

    log("✅ Enriquecimiento completado.")
    return df_enriched


def aggregate_flights(df: pd.DataFrame) -> pd.DataFrame:
    """Agregación simple: cantidad de vuelos por estado."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["flight_status", "count"])
    return df.groupby("flight_status", dropna=False).size().reset_index(name="count")


if __name__ == "__main__":
    # 1) Carga
    df_flights_raw, df_airports_raw = load_data()

    # 2) Transformaciones
    df_clean = clean_flights(df_flights_raw)
    df_enriched = enrich_flights(df_clean, df_airports_raw)

    # 3) Selección de columnas (a prueba de KeyError)
    desired_cols = [
        "flight_date",
        "flight_status",
        "is_delayed",
        "departure_iata",
        "departure_airport_name",
        "arrival_iata",
        "aircraft_icao",
    ]
    existing_cols = [c for c in desired_cols if c in df_enriched.columns]
    missing = [c for c in desired_cols if c not in df_enriched.columns]
    if missing:
        log(f"⚠️ Columnas faltantes en df_enriched: {missing}. Continúo con las existentes.")

    df_final = df_enriched[existing_cols].copy()

    # 4) Partición: asegurar flight_date string YYYY-MM-DD (o 'unknown')
    if "flight_date" in df_final.columns:
        if pd.api.types.is_datetime64_any_dtype(df_final["flight_date"]):
            df_final["flight_date"] = df_final["flight_date"].dt.date.astype("string")
        df_final["flight_date"] = df_final["flight_date"].fillna("unknown").astype("string")
    else:
        df_final["flight_date"] = "unknown"

    # 5) Escritura Delta (hard overwrite para evitar conflictos de schema)
    if os.path.exists(processed_enriched_path):
        try:
            shutil.rmtree(processed_enriched_path)
            log(f"🧹 Eliminada tabla existente en {processed_enriched_path}.")
        except Exception as e:
            log(f"⚠️ No se pudo eliminar {processed_enriched_path}: {e}")

    write_deltalake(processed_enriched_path, df_final, mode="overwrite", partition_by=["flight_date"])
    log(f"✅ Guardado DataFrame final en {processed_enriched_path}")

    # 6) (Opcional) Guardar agregación de estado
    df_agg = aggregate_flights(df_final)
    if not df_agg.empty:
        if os.path.exists(agg_path):
            try:
                shutil.rmtree(agg_path)
                log(f"🧹 Eliminada tabla existente en {agg_path}.")
            except Exception as e:
                log(f"⚠️ No se pudo eliminar {agg_path}: {e}")
        write_deltalake(agg_path, df_agg, mode="overwrite")
        log("✅ Guardada agregación por estado en Delta.")


