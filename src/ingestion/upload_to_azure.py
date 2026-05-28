import json
from datetime import datetime
from typing import List

from azure.storage.blob import BlobServiceClient

import sys
sys.path.insert(0, ".")
from config.settings import AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME
from src.utils.logger import get_logger

logger = get_logger("azure_upload")


class AzureUploader:
    """Upload de dados para Azure Blob Storage no padrão medallion."""

    def __init__(self):
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING não configurada. "
                "Defina no arquivo .env"
            )

        self.blob_service = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        self.container_name = AZURE_CONTAINER_NAME
        self._ensure_container_exists()

    def _ensure_container_exists(self):
        try:
            container_client = self.blob_service.get_container_client(self.container_name)
            container_client.get_container_properties()
            logger.info(f"Container '{self.container_name}' encontrado")
        except Exception:
            self.blob_service.create_container(self.container_name)
            logger.info(f"Container '{self.container_name}' criado")

    def upload_json(self, data: List[dict], source: str, dataset: str) -> str:
        now = datetime.now()
        blob_path = (
            f"bronze/{source}/{dataset}"
            f"/{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}"
            f"/data.json"
        )

        json_content = json.dumps(data, ensure_ascii=False, indent=2)

        blob_client = self.blob_service.get_blob_client(
            container=self.container_name,
            blob=blob_path,
        )
        blob_client.upload_blob(json_content, overwrite=True)

        logger.info(
            f"Upload concluído: {blob_path} ({len(data)} registros, {len(json_content)} bytes)"
        )
        return blob_path

    def upload_all(self, bcb_data: dict, ibge_data: dict) -> List[str]:
        uploaded_paths = []

        for serie_name, records in bcb_data.items():
            if records:
                path = self.upload_json(records, source="bcb", dataset=serie_name)
                uploaded_paths.append(path)

        for agregado_name, records in ibge_data.items():
            if records:
                path = self.upload_json(records, source="ibge", dataset=agregado_name)
                uploaded_paths.append(path)

        logger.info(f"Total de uploads: {len(uploaded_paths)} arquivos")
        return uploaded_paths
