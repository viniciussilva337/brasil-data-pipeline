import os
from dotenv import load_dotenv

load_dotenv()

# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "datalake")

# API do Banco Central (SGS)
BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
BCB_SERIES = {
    "selic": 432,
    "ipca": 433,
    "cambio_usd_brl": 1,
    "cdi": 12,
}

# API do IBGE
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3"
IBGE_AGREGADOS = {
    "pib": 5938,
    "ipca_geral": 1737,
    "populacao": 6579,
}

# Ingestão
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 5
