import types
from unittest.mock import patch, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

# Mock do módulo azure antes de importar o módulo sob teste
azure_mock = types.ModuleType("azure")
azure_storage_mock = types.ModuleType("azure.storage")
azure_blob_mock = types.ModuleType("azure.storage.blob")
azure_blob_mock.BlobServiceClient = MagicMock()
azure_mock.storage = azure_storage_mock
azure_storage_mock.blob = azure_blob_mock

sys.modules.setdefault("azure", azure_mock)
sys.modules.setdefault("azure.storage", azure_storage_mock)
sys.modules.setdefault("azure.storage.blob", azure_blob_mock)

from src.ingestion.upload_to_azure import AzureUploader


def _make_uploader(mock_blob_service_cls, container_exists=True):
    mock_service = MagicMock()
    mock_blob_service_cls.from_connection_string.return_value = mock_service

    container_client = MagicMock()
    if container_exists:
        container_client.get_container_properties.return_value = {}
    else:
        container_client.get_container_properties.side_effect = Exception("Not found")
    mock_service.get_container_client.return_value = container_client

    return mock_service


class TestAzureUploader:
    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", None)
    def test_init_sem_connection_string_raises(self):
        with pytest.raises(ValueError, match="AZURE_STORAGE_CONNECTION_STRING"):
            AzureUploader()

    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", "fake-conn-string")
    @patch("src.ingestion.upload_to_azure.BlobServiceClient")
    def test_init_cria_container_se_nao_existe(self, mock_blob_service_cls):
        mock_service = _make_uploader(mock_blob_service_cls, container_exists=False)
        AzureUploader()
        mock_service.create_container.assert_called_once()

    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", "fake-conn-string")
    @patch("src.ingestion.upload_to_azure.BlobServiceClient")
    def test_init_container_ja_existe(self, mock_blob_service_cls):
        mock_service = _make_uploader(mock_blob_service_cls, container_exists=True)
        AzureUploader()
        mock_service.create_container.assert_not_called()

    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", "fake-conn-string")
    @patch("src.ingestion.upload_to_azure.BlobServiceClient")
    def test_upload_json_path_pattern(self, mock_blob_service_cls):
        mock_service = _make_uploader(mock_blob_service_cls)
        mock_blob_client = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client

        uploader = AzureUploader()
        data = [{"serie": "selic", "valor": 11.75}]
        path = uploader.upload_json(data, source="bcb", dataset="selic")

        assert path.startswith("bronze/bcb/selic/")
        assert path.endswith("/data.json")
        mock_blob_client.upload_blob.assert_called_once()

    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", "fake-conn-string")
    @patch("src.ingestion.upload_to_azure.BlobServiceClient")
    def test_upload_all(self, mock_blob_service_cls):
        mock_service = _make_uploader(mock_blob_service_cls)
        mock_service.get_blob_client.return_value = MagicMock()

        uploader = AzureUploader()

        bcb_data = {
            "selic": [{"valor": 11.75}],
            "ipca": [{"valor": 0.42}],
            "vazio": [],
        }
        ibge_data = {
            "pib": [{"valor": 100}],
            "sem_dados": [],
        }

        paths = uploader.upload_all(bcb_data, ibge_data)

        assert len(paths) == 3
        assert any("bcb/selic" in p for p in paths)
        assert any("bcb/ipca" in p for p in paths)
        assert any("ibge/pib" in p for p in paths)

    @patch("src.ingestion.upload_to_azure.AZURE_STORAGE_CONNECTION_STRING", "fake-conn-string")
    @patch("src.ingestion.upload_to_azure.BlobServiceClient")
    def test_upload_all_sem_dados(self, mock_blob_service_cls):
        _make_uploader(mock_blob_service_cls)
        uploader = AzureUploader()
        paths = uploader.upload_all({}, {})
        assert paths == []
