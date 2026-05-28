import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

import sys
sys.path.insert(0, ".")
from config.settings import BCB_BASE_URL, BCB_SERIES, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY
from src.utils.logger import get_logger

logger = get_logger("bcb_client")


class BCBClient:
    """Cliente para a API SGS do Banco Central do Brasil."""

    def __init__(self):
        self.base_url = BCB_BASE_URL
        self.series = BCB_SERIES
        self.session = requests.Session()

    def fetch_serie(
        self,
        serie_name: str,
        data_inicio: Optional[str] = None,
        data_fim: Optional[str] = None,
    ) -> List[dict]:
        if serie_name not in self.series:
            raise ValueError(
                f"Série '{serie_name}' não encontrada. Disponíveis: {list(self.series.keys())}"
            )

        codigo = self.series[serie_name]

        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
        if not data_fim:
            data_fim = datetime.now().strftime("%d/%m/%Y")

        url = self.base_url.format(codigo=codigo)
        params = {
            "formato": "json",
            "dataInicial": data_inicio,
            "dataFinal": data_fim,
        }

        logger.info(f"Buscando série '{serie_name}' (código {codigo}) de {data_inicio} a {data_fim}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()

                records = [
                    {
                        "serie": serie_name,
                        "codigo": codigo,
                        "data": record["data"],
                        "valor": float(record["valor"]) if record["valor"] else None,
                        "ingested_at": datetime.now().isoformat(),
                    }
                    for record in data
                ]

                logger.info(f"Série '{serie_name}': {len(records)} registros obtidos")
                return records

            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP na tentativa {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Erro de conexão na tentativa {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
            except requests.exceptions.Timeout:
                logger.error(f"Timeout na tentativa {attempt}/{MAX_RETRIES}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
            except (ValueError, KeyError) as e:
                logger.error(f"Erro ao processar resposta: {e}")
                return []

        logger.error(f"Falha ao buscar série '{serie_name}' após {MAX_RETRIES} tentativas")
        return []

    def fetch_all_series(
        self,
        data_inicio: Optional[str] = None,
        data_fim: Optional[str] = None,
    ) -> Dict[str, List[dict]]:
        all_data = {}
        for serie_name in self.series:
            records = self.fetch_serie(serie_name, data_inicio, data_fim)
            all_data[serie_name] = records
            time.sleep(1)
        return all_data
