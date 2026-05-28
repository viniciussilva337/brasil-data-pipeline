import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

import sys
sys.path.insert(0, ".")
from config.settings import IBGE_BASE_URL, IBGE_AGREGADOS, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY
from src.utils.logger import get_logger

logger = get_logger("ibge_client")

IBGE_DEFAULT_NUM_PERIODOS = 6


class IBGEClient:
    """Cliente para a API de Agregados do IBGE."""

    def __init__(self):
        self.base_url = IBGE_BASE_URL
        self.agregados = IBGE_AGREGADOS
        self.session = requests.Session()

    def _fetch_periodos_disponiveis(self, codigo: int) -> List[str]:
        url = f"{self.base_url}/agregados/{codigo}/periodos"
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            periodos = response.json()
            return [p["id"] for p in periodos]
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar períodos do agregado {codigo}: {e}")
            return []

    def fetch_agregado(
        self,
        agregado_name: str,
        num_periodos: int = IBGE_DEFAULT_NUM_PERIODOS,
        localidades: str = "N1[all]",
    ) -> List[dict]:
        if agregado_name not in self.agregados:
            raise ValueError(
                f"Agregado '{agregado_name}' não encontrado. Disponíveis: {list(self.agregados.keys())}"
            )

        codigo = self.agregados[agregado_name]

        todos_periodos = self._fetch_periodos_disponiveis(codigo)
        if not todos_periodos:
            logger.error(f"Nenhum período disponível para '{agregado_name}'")
            return []

        periodos_selecionados = todos_periodos[-num_periodos:]
        periodos_str = "|".join(periodos_selecionados)
        url = f"{self.base_url}/agregados/{codigo}/periodos/{periodos_str}/variaveis?localidades={localidades}"

        logger.info(f"Buscando agregado '{agregado_name}' (código {codigo}), períodos: {periodos_str}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                raw_data = response.json()

                records = self._parse_agregado_response(agregado_name, codigo, raw_data)
                logger.info(f"Agregado '{agregado_name}': {len(records)} registros obtidos")
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

        logger.error(f"Falha ao buscar agregado '{agregado_name}' após {MAX_RETRIES} tentativas")
        return []

    def _parse_agregado_response(
        self, agregado_name: str, codigo: int, raw_data: list
    ) -> List[dict]:
        records = []
        for variavel in raw_data:
            variavel_id = variavel.get("id")
            variavel_nome = variavel.get("variavel")

            for resultado in variavel.get("resultados", []):
                for serie in resultado.get("series", []):
                    localidade = serie.get("localidade", {})
                    for periodo, valor in serie.get("serie", {}).items():
                        records.append({
                            "agregado": agregado_name,
                            "codigo_agregado": codigo,
                            "variavel_id": variavel_id,
                            "variavel_nome": variavel_nome,
                            "localidade_id": localidade.get("id"),
                            "localidade_nome": localidade.get("nome"),
                            "localidade_nivel": localidade.get("nivel", {}).get("nome"),
                            "periodo": periodo,
                            "valor": valor if valor != "..." else None,
                            "ingested_at": datetime.now().isoformat(),
                        })
        return records

    def fetch_localidades_estados(self) -> List[dict]:
        url = f"{self.base_url.replace('/v3', '/v1')}/localidades/estados"
        logger.info("Buscando lista de estados")

        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            estados = response.json()

            records = [
                {
                    "id": estado["id"],
                    "sigla": estado["sigla"],
                    "nome": estado["nome"],
                    "regiao_id": estado["regiao"]["id"],
                    "regiao_nome": estado["regiao"]["nome"],
                    "ingested_at": datetime.now().isoformat(),
                }
                for estado in estados
            ]

            logger.info(f"Estados: {len(records)} registros obtidos")
            return records

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar estados: {e}")
            return []

    def fetch_all_agregados(self) -> Dict[str, List[dict]]:
        all_data = {}
        for agregado_name in self.agregados:
            records = self.fetch_agregado(agregado_name)
            all_data[agregado_name] = records
            time.sleep(1)

        all_data["estados"] = self.fetch_localidades_estados()
        return all_data
