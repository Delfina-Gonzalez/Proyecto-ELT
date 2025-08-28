import pandas as pd

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas duplicadas de un DataFrame de Pandas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.

    Returns:
        pd.DataFrame: Un nuevo DataFrame sin filas duplicadas.
    """
    return df.drop_duplicates()

def handle_nulls(df: pd.DataFrame, cols: list, fill_value=None) -> pd.DataFrame:
    """
    Rellena los valores nulos en columnas específicas de un DataFrame.

    Esta función toma un DataFrame, una lista de nombres de columnas y un valor
    para reemplazar los valores nulos en esas columnas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        cols (list): Una lista de los nombres de las columnas a procesar.
        fill_value (any, opcional): El valor con el que se llenarán los nulos.
            Por defecto es None.

    Returns:
        pd.DataFrame: El DataFrame con los valores nulos rellenados.
    """
    return df.fillna({col: fill_value for col in cols})

def convert_column_types(df: pd.DataFrame, type_mappings: dict) -> pd.DataFrame:
    """
    Convierte el tipo de datos de columnas de un DataFrame.

    La función itera sobre un diccionario de mapeos de columnas a tipos de datos,
    intentando convertir cada columna a su tipo de destino. Si la conversión
    falla, imprime un mensaje de error y continúa.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        type_mappings (dict): Un diccionario donde las claves son los nombres de
            las columnas y los valores son los tipos de datos a los que se
            quieren convertir.

    Returns:
        pd.DataFrame: El DataFrame con los tipos de datos de las columnas convertidos.
    """
    for col, dtype in type_mappings.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception as e:
            print(f"No se pudo convertir la columna {col} a {dtype}: {e}")
    return df

def rename_columns(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    """
    Renombra columnas de un DataFrame de Pandas.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        rename_map (dict): Un diccionario donde las claves son los nombres
            actuales de las columnas y los valores son los nuevos nombres.

    Returns:
        pd.DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    return df.rename(columns=rename_map)

def create_new_column(df: pd.DataFrame, new_col: str, func) -> pd.DataFrame:
    """
    Crea una nueva columna en un DataFrame aplicando una función a cada fila.

    Args:
        df (pd.DataFrame): El DataFrame de entrada.
        new_col (str): El nombre de la nueva columna a crear.
        func (callable): Una función que se aplicará a cada fila del DataFrame.

    Returns:
        pd.DataFrame: El DataFrame con la nueva columna agregada.
    """
    df[new_col] = df.apply(func, axis=1)
    return df

def join_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key: str, how="left") -> pd.DataFrame:
    """
    Combina dos DataFrames de Pandas basados en una columna clave.

    Utiliza el método `merge` para unir dos DataFrames de manera similar a un
    JOIN de SQL.

    Args:
        df1 (pd.DataFrame): El DataFrame izquierdo.
        df2 (pd.DataFrame): El DataFrame derecho.
        key (str): El nombre de la columna clave en la que se realizará el JOIN.
        how (str, opcional): El tipo de join a realizar ('left', 'right', 'outer', 'inner').
            Por defecto es 'left'.

    Returns:
        pd.DataFrame: El DataFrame resultante de la unión.
    """
    return df1.merge(df2, on=key, how=how)
