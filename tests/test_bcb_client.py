import json
from unittest.mock import patch, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from src.ingestion.bcb_client import BCBClient


@pytest.fixture
def client():
    return BCBClient()


@pytest.fixture
def mock_bcb_response():
    return [
        {"data": "01/01/2024", "valor": "11.75"},
        {"data": "02/01/2024", "valor": "11.75"},
        {"data": "03/01/2024", "valor": "11.25"},
    ]


class TestBCBClient:
    def test_series_disponiveis(self, client):
        assert "selic" in client.series
        assert "ipca" in client.series
        assert "cambio_usd_brl" in client.series
        assert "cdi" in client.series

    def test_serie_invalida_raises(self, client):
        with pytest.raises(ValueError, match="não encontrada"):
            client.fetch_serie("serie_inexistente")

    @patch("src.ingestion.bcb_client.requests.Session.get")
    def test_fetch_serie_sucesso(self, mock_get, client, mock_bcb_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_bcb_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        records = client.fetch_serie("selic", "01/01/2024", "03/01/2024")

        assert len(records) == 3
        assert records[0]["serie"] == "selic"
        assert records[0]["codigo"] == 432
        assert records[0]["valor"] == 11.75
        assert "ingested_at" in records[0]

    @patch("src.ingestion.bcb_client.requests.Session.get")
    def test_fetch_serie_valor_none(self, mock_get, client):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"data": "01/01/2024", "valor": ""},
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        records = client.fetch_serie("selic")

        assert len(records) == 1
        assert records[0]["valor"] is None

    @patch("src.ingestion.bcb_client.requests.Session.get")
    def test_fetch_serie_erro_http_retries(self, mock_get, client):
        from requests.exceptions import HTTPError

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("500 Server Error")
        mock_get.return_value = mock_response

        with patch("src.ingestion.bcb_client.MAX_RETRIES", 2), \
             patch("src.ingestion.bcb_client.RETRY_DELAY", 0):
            records = client.fetch_serie("selic")

        assert records == []
        assert mock_get.call_count == 2

    @patch("src.ingestion.bcb_client.requests.Session.get")
    def test_fetch_all_series(self, mock_get, client, mock_bcb_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_bcb_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with patch("src.ingestion.bcb_client.time.sleep"):
            all_data = client.fetch_all_series()

        assert len(all_data) == 4
        assert all(len(records) == 3 for records in all_data.values())
