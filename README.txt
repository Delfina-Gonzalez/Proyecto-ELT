# Pipeline de Datos: Extracción, Carga y Transformación (ETL)

Este proyecto implementa un *pipeline* de datos modular para procesar información de vuelos utilizando la API de AviationStack. Su objetivo es demostrar la extracción de datos, su almacenamiento en un **Data Lake** con formato **Delta Lake**, y su posterior procesamiento para obtener un conjunto de datos limpio y enriquecido.

---

### **1. Resumen del Proyecto y Funcionalidades**

El *pipeline* está dividido en tres etapas principales, cada una manejada por un script de Python en la carpeta `src/`.

* **Extracción (`extract.py`):** Obtiene datos de vuelos (temporales) y aeropuertos (estáticos) de la API de AviationStack. Los datos crudos se guardan en formato **Parquet**.
* **Carga (`load.py`):** Lee los archivos Parquet y los carga en un **Data Lake** en formato **Delta Lake**. La estrategia de carga es **incremental (`append`)** para los datos de vuelos y **completa (`overwrite`)** para los datos de aeropuertos, lo que optimiza el almacenamiento y el rendimiento.
* **Transformación (`transform.py`):** Procesa los datos de la capa Delta Lake con **Pandas**, aplicando **más de cuatro tipos de transformaciones** para limpiar y enriquecer el conjunto de datos. El resultado final se guarda en una nueva tabla Delta Lake procesada.

---

### **2. Estructura del Proyecto**

La organización del proyecto sigue una estructura modular para facilitar su comprensión y mantenimiento.

```text
.
├── data_lake/
│   ├── flights/
│   │   ├── processed/          # Tabla Delta Lake con datos crudos (vuelos)
│   │   └── processed_enriched_pandas/ # Tabla Delta Lake con datos procesados (vuelos)
│   └── airports/
│       └── processed/          # Tabla Delta Lake con datos estáticos (aeropuertos)
├── src/
│   ├── extract.py              # Consulta a la API de AviationStack
│   ├── load.py                 # Carga los datos en Delta Lake
│   ├── transform.py            # Realiza las transformaciones con Pandas
│   └── utils.py                # Funciones de ayuda y utilidades
│   └── notebook.ipynb          # Notebook para demostrar el pipeline
├── .gitignore                  # Archivos a ignorar por Git
├── LICENSE                     # Licencia del proyecto (MIT)
├── pipeline.config.example     # Plantilla de configuración para API Key
├── README.md                   # Descripción del proyecto (este archivo)
└── requirements.txt            # Dependencias del proyecto

### **Pasos para la Descarga y Ejecución**

Sigue estos sencillos pasos para poner el proyecto en marcha en tu entorno local.

1.  **Clona el repositorio:**
    Abre tu terminal o línea de comandos, navega hasta el directorio donde deseas guardar el proyecto y ejecuta el siguiente comando para clonar el repositorio de GitHub.
    ```bash
    git clone [https://github.com/Delfina-Gonzalez/Proyecto-ELT.git](https://github.com/Delfina-Gonzalez/Proyecto-ELT.git)
    cd Proyecto-ELT
    ```
2.  **Configura el entorno virtual:**
    Se recomienda usar un entorno virtual para gestionar las dependencias del proyecto de forma aislada.
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # En Linux/macOS
    # O en Windows: venv\Scripts\activate
    ```
3.  **Instala las dependencias:**
    El archivo `requirements.txt` contiene todas las librerías necesarias para que el pipeline funcione.
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configura la API Key:**
    Como el archivo `pipeline.config` está en el `.gitignore`, no se clona automáticamente. Necesitas crearlo manualmente para que los scripts puedan acceder a tu clave de la API.

    Crea un archivo llamado `pipeline.config` en el directorio principal del proyecto (`Proyecto-ELT/`). Luego, agrega el siguiente contenido, reemplazando `TU_API_KEY_AQUI` con la clave que te proporcionó AviationStack. Podés tomar de ejemplo el archivo "pipeline.config.example".
    
    ```ini
    [DEFAULT]
    AVIATIONSTACK_API_KEY=TU_API_KEY_AQUI
    ```

5.  **Ejecuta el pipeline con el notebook:**
    Abre el archivo `notebook.ipynb` en Jupyter Notebook o Google Colab y ejecuta cada celda de forma secuencial. Este *notebook* está diseñado para guiarte a través de cada etapa del pipeline, mostrando los resultados y justificando las decisiones de diseño.