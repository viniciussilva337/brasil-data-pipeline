from unittest.mock import patch, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from src.ingestion.ibge_client import IBGEClient


@pytest.fixture
def client():
    return IBGEClient()


@pytest.fixture
def mock_ibge_agregado_response():
    return [
        {
            "id": "63",
            "variavel": "IPCA - Variação mensal",
            "resultados": [
                {
                    "series": [
                        {
                            "localidade": {
                                "id": "1",
                                "nome": "Brasil",
                                "nivel": {"nome": "País"},
                            },
                            "serie": {
                                "202401": "0.42",
                                "202402": "0.83",
                            },
                        }
                    ]
                }
            ],
        }
    ]


@pytest.fixture
def mock_estados_response():
    return [
        {
            "id": 35,
            "sigla": "SP",
            "nome": "São Paulo",
            "regiao": {"id": 3, "nome": "Sudeste"},
        },
        {
            "id": 33,
            "sigla": "RJ",
            "nome": "Rio de Janeiro",
            "regiao": {"id": 3, "nome": "Sudeste"},
        },
    ]


class TestIBGEClient:
    def test_agregados_disponiveis(self, client):
        assert "pib" in client.agregados
        assert "ipca_geral" in client.agregados
        assert "populacao" in client.agregados

    def test_agregado_invalido_raises(self, client):
        with pytest.raises(ValueError, match="não encontrado"):
            client.fetch_agregado("agregado_inexistente")

    @patch("src.ingestion.ibge_client.requests.Session.get")
    def test_fetch_agregado_sucesso(self, mock_get, client, mock_ibge_agregado_response):
        mock_periodos = MagicMock()
        mock_periodos.json.return_value = [
            {"id": "202401"}, {"id": "202402"}, {"id": "202403"},
            {"id": "202404"}, {"id": "202405"}, {"id": "202406"},
        ]
        mock_periodos.raise_for_status.return_value = None

        mock_data = MagicMock()
        mock_data.json.return_value = mock_ibge_agregado_response
        mock_data.raise_for_status.return_value = None

        mock_get.side_effect = [mock_periodos, mock_data]

        records = client.fetch_agregado("ipca_geral")

        assert len(records) == 2
        assert records[0]["agregado"] == "ipca_geral"
        assert records[0]["localidade_nome"] == "Brasil"
        assert records[0]["periodo"] == "202401"
        assert records[0]["valor"] == "0.42"

    @patch("src.ingestion.ibge_client.requests.Session.get")
    def test_fetch_localidades_estados(self, mock_get, client, mock_estados_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_estados_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        records = client.fetch_localidades_estados()

        assert len(records) == 2
        assert records[0]["sigla"] == "SP"
        assert records[0]["regiao_nome"] == "Sudeste"
        assert "ingested_at" in records[0]

    @patch("src.ingestion.ibge_client.requests.Session.get")
    def test_fetch_agregado_erro_http(self, mock_get, client):
        from requests.exceptions import HTTPError

        mock_periodos = MagicMock()
        mock_periodos.json.return_value = [{"id": "2023"}]
        mock_periodos.raise_for_status.return_value = None

        mock_data = MagicMock()
        mock_data.raise_for_status.side_effect = HTTPError("404 Not Found")

        mock_get.side_effect = [mock_periodos, mock_data, mock_data]

        with patch("src.ingestion.ibge_client.MAX_RETRIES", 2), \
             patch("src.ingestion.ibge_client.RETRY_DELAY", 0):
            records = client.fetch_agregado("pib")

        assert records == []

    @patch("src.ingestion.ibge_client.requests.Session.get")
    def test_fetch_agregado_sem_periodos(self, mock_get, client):
        from requests.exceptions import ConnectionError

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = ConnectionError("Offline")
        mock_get.return_value = mock_response

        records = client.fetch_agregado("pib")
        assert records == []

    @patch("src.ingestion.ibge_client.requests.Session.get")
    def test_fetch_periodos_disponiveis(self, mock_get, client):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": "2020"}, {"id": "2021"}, {"id": "2022"}, {"id": "2023"},
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        periodos = client._fetch_periodos_disponiveis(5938)
        assert periodos == ["2020", "2021", "2022", "2023"]

    def test_parse_agregado_dados_incompletos(self, client):
        raw = [
            {
                "id": "1",
                "variavel": "Teste",
                "resultados": [
                    {
                        "series": [
                            {
                                "localidade": {"id": "1", "nome": "Brasil", "nivel": {"nome": "País"}},
                                "serie": {"202401": "..."},
                            }
                        ]
                    }
                ],
            }
        ]

        records = client._parse_agregado_response("teste", 999, raw)

        assert len(records) == 1
        assert records[0]["valor"] is None
