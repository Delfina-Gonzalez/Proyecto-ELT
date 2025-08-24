import os
import time
import requests
import pandas as pd
from utils import log, ensure_dir
from typing import Optional, Dict

def extract_data(endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> pd.DataFrame:
    url = f"{BASE_URL}/{endpoint}"
    params = params or {}
    params["access_key"] = API_KEY

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json().get("data", [])
                if not data:
                    log(f"⚠️ {endpoint}: sin datos en la respuesta.")
                return pd.DataFrame(data)

            elif response.status_code == 401:
                log("❌ API Key inválida o no configurada. Revisa AVIATIONSTACK_API_KEY.")
                break

            elif response.status_code == 429:
                log(f"⚠️ Límite de rate en {endpoint}. Reintento {attempt}/{max_retries}...")
                time.sleep(2 * attempt)

            else:
                log(f"❌ Error {response.status_code} en {endpoint}: {response.text}")
                break

        except Exception as e:
            log(f"❌ Excepción en {endpoint}, intento {attempt}: {e}")
            time.sleep(2 * attempt)

    log(f"⚠️ No se pudo extraer datos de {endpoint} tras {max_retries} intentos.")
    return pd.DataFrame()
