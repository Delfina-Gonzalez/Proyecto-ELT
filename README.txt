# ELT con AviationStack API

Este proyecto implementa un pipeline **ELT (Extract, Load, Transform)** que consume datos desde la **AviationStack API**, una API pública REST que provee información detallada sobre vuelos, aeropuertos y aerolíneas a nivel mundial.

##  Características
- Extracción de datos vía HTTP/JSON desde AviationStack.
- Soporte para:
  - Vuelos en tiempo real (latencia ~30–60 s).
  - Datos históricos (últimos 3 meses).
  - Aeropuertos, aerolíneas, rutas, tipos de aeronaves, ciudades, impuestos.
- Carga a formato **Delta Lake** o CSV mediante `pandas`.
- Posibles transformaciones (normalización de datos, limpieza, filtros).

##  Requisitos
- Python 3.9+
- `pandas`
- `requests`
- `delta-spark` (opcional si usás Delta Lake)

Instalación:
```bash
pip install -r requirements.txt

Uso

Clonar el repositorio:

git clone https://github.com/usuario/proyecto-elt-aviationstack.git
cd proyecto-elt-aviationstack


Crear y activar entorno virtual:

python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows


Obtener clave de API:

Registrarse en AviationStack
 y obtener el access_key.

Ejecutar el pipeline:

export AVIATIONSTACK_ACCESS_KEY=tu_api_key
python main.py

Estructura del proyecto
.
├── src/
│   ├── extract.py       # Consulta a AviationStack
│   ├── load.py          # Carga a Delta Lake o CSV
│   ├── transform.py     # Transformaciones de datos
│   └── utils.py         # Helpers
├── data/                # Datos crudos y transformados
├── requirements.txt
├── LICENSE
└── README.md

Recursos

Documentación de la API
 
aviationstack.com

Plan gratuito: ideal para pruebas. Planes pagos permiten más funcionalidades y request volume 
aviationstack.com
+1
.

Contribuciones

¡Contribuciones bienvenidas! Abrí un issue o vas por un pull request 🛠️.