import logging

def setup_logger():
    """
    Configura un logger con un formato estándar para el proyecto.

    Esta función inicializa la configuración básica del módulo de `logging`
    de Python, estableciendo el nivel a `INFO` y un formato de salida
    que incluye la fecha y hora, el nivel de severidad y el mensaje.
    Luego, devuelve una instancia de logger con el nombre "ProyectoELT".

    Returns:
        logging.Logger: Una instancia de logger configurada.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("ProyectoELT")
