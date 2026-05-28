# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Dados (Camada Bronze)
# MAGIC Coleta dados das APIs do Banco Central e IBGE e salva no DBFS como JSON.

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingestão do Banco Central (SGS)

# COMMAND ----------

BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
BCB_SERIES = {"selic": 432, "ipca": 433, "cambio_usd_brl": 1, "cdi": 12}

data_inicio = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
data_fim = datetime.now().strftime("%d/%m/%Y")

bcb_data = {}

for serie_name, codigo in BCB_SERIES.items():
    url = BCB_BASE_URL.format(codigo=codigo)
    params = {"formato": "json", "dataInicial": data_inicio, "dataFinal": data_fim}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        raw = response.json()

        records = [
            {
                "serie": serie_name,
                "codigo": codigo,
                "data": r["data"],
                "valor": float(r["valor"]) if r["valor"] else None,
                "ingested_at": datetime.now().isoformat(),
            }
            for r in raw
        ]
        bcb_data[serie_name] = records
        print(f"BCB - {serie_name}: {len(records)} registros")
    except Exception as e:
        print(f"BCB - {serie_name}: ERRO - {e}")
        bcb_data[serie_name] = []

    time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestão do IBGE

# COMMAND ----------

IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3"
IBGE_AGREGADOS = {"pib": 5938, "ipca_geral": 1737, "populacao": 6579}

ibge_data = {}

for agregado_name, codigo in IBGE_AGREGADOS.items():
    # Buscar períodos disponíveis
    try:
        resp_periodos = requests.get(f"{IBGE_BASE_URL}/agregados/{codigo}/periodos", timeout=30)
        resp_periodos.raise_for_status()
        todos_periodos = [p["id"] for p in resp_periodos.json()]
        periodos_str = "|".join(todos_periodos[-6:])
    except Exception as e:
        print(f"IBGE - {agregado_name}: ERRO ao buscar períodos - {e}")
        ibge_data[agregado_name] = []
        continue

    url = f"{IBGE_BASE_URL}/agregados/{codigo}/periodos/{periodos_str}/variaveis?localidades=N1[all]"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        raw = response.json()

        records = []
        for variavel in raw:
            for resultado in variavel.get("resultados", []):
                for serie in resultado.get("series", []):
                    localidade = serie.get("localidade", {})
                    for periodo, valor in serie.get("serie", {}).items():
                        records.append({
                            "agregado": agregado_name,
                            "codigo_agregado": codigo,
                            "variavel_id": variavel.get("id"),
                            "variavel_nome": variavel.get("variavel"),
                            "localidade_id": localidade.get("id"),
                            "localidade_nome": localidade.get("nome"),
                            "localidade_nivel": localidade.get("nivel", {}).get("nome"),
                            "periodo": periodo,
                            "valor": valor if valor != "..." else None,
                            "ingested_at": datetime.now().isoformat(),
                        })

        ibge_data[agregado_name] = records
        print(f"IBGE - {agregado_name}: {len(records)} registros")
    except Exception as e:
        print(f"IBGE - {agregado_name}: ERRO - {e}")
        ibge_data[agregado_name] = []

    time.sleep(1)

# Estados
try:
    resp = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=30)
    resp.raise_for_status()
    estados = [
        {
            "id": e["id"], "sigla": e["sigla"], "nome": e["nome"],
            "regiao_id": e["regiao"]["id"], "regiao_nome": e["regiao"]["nome"],
            "ingested_at": datetime.now().isoformat(),
        }
        for e in resp.json()
    ]
    ibge_data["estados"] = estados
    print(f"IBGE - estados: {len(estados)} registros")
except Exception as e:
    print(f"IBGE - estados: ERRO - {e}")
    ibge_data["estados"] = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Salvar na camada Bronze (DBFS)

# COMMAND ----------

base_path = "/FileStore/brasil_pipeline"
now = datetime.now()
date_path = now.strftime("%Y/%m/%d")

paths = []

for serie_name, records in bcb_data.items():
    if records:
        blob_path = f"{base_path}/bronze/bcb/{serie_name}/{date_path}/data.json"
        dbutils.fs.put(blob_path, json.dumps(records, ensure_ascii=False, indent=2), overwrite=True)
        paths.append(blob_path)
        print(f"Salvo: {blob_path} ({len(records)} registros)")

for dataset_name, records in ibge_data.items():
    if records:
        blob_path = f"{base_path}/bronze/ibge/{dataset_name}/{date_path}/data.json"
        dbutils.fs.put(blob_path, json.dumps(records, ensure_ascii=False, indent=2), overwrite=True)
        paths.append(blob_path)
        print(f"Salvo: {blob_path} ({len(records)} registros)")

print(f"\nTotal: {len(paths)} arquivos salvos na camada Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar dados salvos

# COMMAND ----------

display(dbutils.fs.ls(f"{base_path}/bronze/bcb/"))
display(dbutils.fs.ls(f"{base_path}/bronze/ibge/"))
