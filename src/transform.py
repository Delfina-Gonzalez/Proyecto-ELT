import pandas as pd

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def handle_nulls(df: pd.DataFrame, cols: list, fill_value=None) -> pd.DataFrame:
    return df.fillna({col: fill_value for col in cols})

def convert_column_types(df: pd.DataFrame, type_mappings: dict) -> pd.DataFrame:
    for col, dtype in type_mappings.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception as e:
            print(f"No se pudo convertir la columna {col} a {dtype}: {e}")
    return df

def rename_columns(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    return df.rename(columns=rename_map)

def create_new_column(df: pd.DataFrame, new_col: str, func) -> pd.DataFrame:
    df[new_col] = df.apply(func, axis=1)
    return df

def join_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key: str, how="left") -> pd.DataFrame:
    return df1.merge(df2, on=key, how=how)
