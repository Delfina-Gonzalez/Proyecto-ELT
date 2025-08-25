
# 📘 Proyecto ETL - Pipeline de Datos con AviationStack

Este proyecto implementa un *pipeline* modular en Python para extraer, almacenar y transformar datos de vuelos usando la API de **AviationStack**.  

El pipeline demuestra:  
- **Extracción** desde dos endpoints (`flights` = datos temporales, `airports` = metadatos).  
- **Almacenamiento en Delta Lake**, con estrategias incremental y full.  
- **Procesamiento con Pandas**, aplicando limpieza, normalización, enriquecimiento y agregaciones.  

---

## 📂 Estructura del Proyecto

```text
.
├── data_lake/                # Data Lake local (Delta Lake)
├── src/
│   ├── extract.py             # Extracción desde la API (incremental y full)
│   ├── load.py                # Carga desde Parquet → Delta Lake
│   ├── transform.py           # Procesamiento y enriquecimiento
│   ├── utils.py               # Funciones auxiliares
│   └── notebook.ipynb         # Demostración del pipeline paso a paso
├── pipeline.config            # Configuración local no versionable (API key, paths, etc.)
├── pipeline.config.example    # Ejemplo de configuración
├── requirements.txt           # Dependencias
├── README.md                  # Este archivo
└── LICENSE
```

---

## 🚀 Cómo empezar

### 1. Clonar el repositorio
```bash
git clone https://github.com/Delfina-Gonzalez/Proyecto-ELT.git
cd Proyecto-ELT
```

### 2. Crear y activar entorno virtual
```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
# o en Windows:
venv\Scripts\activate
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4. Configurar API Key
El archivo `pipeline.config` debe contener tu clave de AviationStack:

```ini
[aviationstack]
api_key = TU_API_KEY_AQUI

[urls]
api_url_base = http://api.aviationstack.com/v1

[paths]
data_lake = ./data_lake

[state]
last_flights_run = 2020-01-01
```

🔑 También podés definir la clave como variable de entorno:  
```bash
export AVIATIONSTACK_API_KEY="TU_API_KEY_AQUI"   # Linux/macOS
set AVIATIONSTACK_API_KEY=TU_API_KEY_AQUI        # Windows
```
(Tendrá prioridad sobre lo que figure en `pipeline.config`).

---

## ▶️ Checklist de Ejecución

1. **Extracción**  
   ```bash
   python src/extract.py
   ```
   - Vuelos → extracción incremental (particionados por `flight_date` o fallback sin filtro).  
   - Aeropuertos → extracción full.  
   - Se guarda en **Parquet** dentro de `data_lake/.../processed`.

2. **Carga a Delta Lake**  
   ```bash
   python src/load.py
   ```
   - Convierte los Parquet a **Delta Lake**.  
   - Vuelos = append incremental.  
   - Aeropuertos = overwrite completo.

3. **Transformación y enriquecimiento**  
   ```bash
   python src/transform.py
   ```
   - Aplana columnas JSON.  
   - Renombra y estandariza campos.  
   - Enriquecimiento con joins (aeropuertos origen/destino).  
   - Genera columna `is_delayed`.  
   - Agregación por `status`.  
   - Guarda en Delta Lake (`processed_enriched` y `agg_by_status`).

4. **Validación y exploración**  
   - Abrir `notebook.ipynb` y ejecutar celdas para ver resultados.

---

## 🧩 Estrategias aplicadas

- **Extracción:**  
  - Incremental = vuelos (por fecha).  
  - Full = aeropuertos.  
- **Carga:**  
  - Incremental (append) = vuelos.  
  - Overwrite completo = aeropuertos.  
- **Procesamiento:**  
  - Aplanado de JSON.  
  - Renombrado de columnas.  
  - Creación de campos derivados (`is_delayed`).  
  - Joins con tablas de referencia.  
  - Group by con agregaciones.

---


---

## 🧪 Uso de archivo de prueba (sin API)

En caso de que la API de AviationStack no funcione (ej. plan free limitado, error 403), se incluye un archivo de ejemplo **`flights_raw_sample.csv`** con datos ficticios de vuelos.

---
