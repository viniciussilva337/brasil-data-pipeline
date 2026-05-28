import types
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, ".")

azure_mock = types.ModuleType("azure")
azure_storage_mock = types.ModuleType("azure.storage")
azure_blob_mock = types.ModuleType("azure.storage.blob")
azure_blob_mock.BlobServiceClient = MagicMock()
azure_mock.storage = azure_storage_mock
azure_storage_mock.blob = azure_blob_mock

sys.modules.setdefault("azure", azure_mock)
sys.modules.setdefault("azure.storage", azure_storage_mock)
sys.modules.setdefault("azure.storage.blob", azure_blob_mock)

from src.main import run_pipeline


class TestRunPipeline:
    @patch("src.main.AzureUploader")
    @patch("src.main.IBGEClient")
    @patch("src.main.BCBClient")
    def test_pipeline_completo(self, mock_bcb_cls, mock_ibge_cls, mock_uploader_cls):
        mock_bcb = MagicMock()
        mock_bcb.fetch_all_series.return_value = {
            "selic": [{"valor": 11.75}, {"valor": 11.50}],
            "ipca": [{"valor": 0.42}],
        }
        mock_bcb_cls.return_value = mock_bcb

        mock_ibge = MagicMock()
        mock_ibge.fetch_all_agregados.return_value = {
            "pib": [{"valor": 100}],
            "estados": [{"sigla": "SP"}, {"sigla": "RJ"}],
        }
        mock_ibge_cls.return_value = mock_ibge

        mock_uploader = MagicMock()
        mock_uploader.upload_all.return_value = [
            "bronze/bcb/selic/2024/01/01/data.json",
            "bronze/bcb/ipca/2024/01/01/data.json",
            "bronze/ibge/pib/2024/01/01/data.json",
            "bronze/ibge/estados/2024/01/01/data.json",
        ]
        mock_uploader_cls.return_value = mock_uploader

        result = run_pipeline()

        assert result["bcb_records"] == 3
        assert result["ibge_records"] == 3
        assert result["uploaded_files"] == 4
        assert len(result["paths"]) == 4
        assert result["elapsed_seconds"] >= 0

        mock_bcb.fetch_all_series.assert_called_once()
        mock_ibge.fetch_all_agregados.assert_called_once()
        mock_uploader.upload_all.assert_called_once()

    @patch("src.main.AzureUploader")
    @patch("src.main.IBGEClient")
    @patch("src.main.BCBClient")
    def test_pipeline_sem_dados(self, mock_bcb_cls, mock_ibge_cls, mock_uploader_cls):
        mock_bcb = MagicMock()
        mock_bcb.fetch_all_series.return_value = {}
        mock_bcb_cls.return_value = mock_bcb

        mock_ibge = MagicMock()
        mock_ibge.fetch_all_agregados.return_value = {}
        mock_ibge_cls.return_value = mock_ibge

        mock_uploader = MagicMock()
        mock_uploader.upload_all.return_value = []
        mock_uploader_cls.return_value = mock_uploader

        result = run_pipeline()

        assert result["bcb_records"] == 0
        assert result["ibge_records"] == 0
        assert result["uploaded_files"] == 0
