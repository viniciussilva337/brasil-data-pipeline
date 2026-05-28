import sys
sys.path.insert(0, ".")

from datetime import datetime
from src.ingestion.bcb_client import BCBClient
from src.ingestion.ibge_client import IBGEClient
from src.ingestion.upload_to_azure import AzureUploader
from src.utils.logger import get_logger

logger = get_logger("pipeline")


def run_pipeline():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE DE INGESTÃO")
    logger.info("=" * 60)

    # 1. Ingestão BCB
    logger.info("--- Etapa 1: Ingestão de dados do Banco Central ---")
    bcb = BCBClient()
    bcb_data = bcb.fetch_all_series()

    bcb_total = sum(len(records) for records in bcb_data.values())
    logger.info(f"BCB: {bcb_total} registros coletados de {len(bcb_data)} séries")

    # 2. Ingestão IBGE
    logger.info("--- Etapa 2: Ingestão de dados do IBGE ---")
    ibge = IBGEClient()
    ibge_data = ibge.fetch_all_agregados()

    ibge_total = sum(len(records) for records in ibge_data.values())
    logger.info(f"IBGE: {ibge_total} registros coletados de {len(ibge_data)} datasets")

    # 3. Upload para Azure
    logger.info("--- Etapa 3: Upload para Azure Blob Storage ---")
    uploader = AzureUploader()
    paths = uploader.upload_all(bcb_data, ibge_data)

    # Resumo
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("PIPELINE CONCLUÍDO")
    logger.info(f"  Registros totais: {bcb_total + ibge_total}")
    logger.info(f"  Arquivos enviados: {len(paths)}")
    logger.info(f"  Tempo de execução: {elapsed:.1f}s")
    logger.info("=" * 60)

    return {
        "bcb_records": bcb_total,
        "ibge_records": ibge_total,
        "uploaded_files": len(paths),
        "paths": paths,
        "elapsed_seconds": elapsed,
    }


if __name__ == "__main__":
    run_pipeline()
