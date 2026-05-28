# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver
# MAGIC Leitura dos dados brutos (JSON) do Azure Blob Storage, limpeza e padronização.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Azure Blob Storage

# COMMAND ----------

storage_account = "stbrasilpipeline"
container = "datalake"
access_key = dbutils.secrets.get(scope="brasil-pipeline", key="storage-access-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    access_key,
)

base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Processamento dos dados do Banco Central

# COMMAND ----------

bcb_series = ["selic", "ipca", "cambio_usd_brl", "cdi"]
bcb_frames = []

for serie in bcb_series:
    path = f"{base_path}/bronze/bcb/{serie}/*/*/*/*"
    try:
        df = spark.read.json(path)
        bcb_frames.append(df)
        print(f"BCB - {serie}: {df.count()} registros lidos")
    except Exception as e:
        print(f"BCB - {serie}: não encontrado ({e})")

# COMMAND ----------

if bcb_frames:
    df_bcb_bronze = bcb_frames[0]
    for df in bcb_frames[1:]:
        df_bcb_bronze = df_bcb_bronze.unionByName(df, allowMissingColumns=True)

    df_bcb_silver = (
        df_bcb_bronze
        .withColumn("valor", F.col("valor").cast(DoubleType()))
        .withColumn("data_referencia", F.to_date(F.col("data"), "dd/MM/yyyy"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["serie", "data"])
        .filter(F.col("valor").isNotNull())
        .select(
            "serie",
            "codigo",
            "data_referencia",
            "valor",
            "ingested_at",
            "processed_at",
        )
        .orderBy("serie", "data_referencia")
    )

    print(f"\nBCB Silver: {df_bcb_silver.count()} registros após limpeza")
    df_bcb_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Processamento dos dados do IBGE

# COMMAND ----------

ibge_datasets = ["pib", "ipca_geral", "populacao", "estados"]
ibge_frames = {}

for dataset in ibge_datasets:
    path = f"{base_path}/bronze/ibge/{dataset}/*/*/*/*"
    try:
        df = spark.read.json(path)
        ibge_frames[dataset] = df
        print(f"IBGE - {dataset}: {df.count()} registros lidos")
    except Exception as e:
        print(f"IBGE - {dataset}: não encontrado ({e})")

# COMMAND ----------

for dataset_name, df in ibge_frames.items():
    if dataset_name == "estados":
        df_silver = (
            df
            .withColumn("ingested_at", F.to_timestamp("ingested_at"))
            .withColumn("processed_at", F.current_timestamp())
            .dropDuplicates(["id"])
            .select("id", "sigla", "nome", "regiao_id", "regiao_nome", "ingested_at", "processed_at")
        )
    else:
        df_silver = (
            df
            .withColumn("valor", F.col("valor").cast(DoubleType()))
            .withColumn("ingested_at", F.to_timestamp("ingested_at"))
            .withColumn("processed_at", F.current_timestamp())
            .dropDuplicates(["agregado", "variavel_id", "localidade_id", "periodo"])
            .select(
                "agregado",
                "codigo_agregado",
                "variavel_id",
                "variavel_nome",
                "localidade_id",
                "localidade_nome",
                "localidade_nivel",
                "periodo",
                "valor",
                "ingested_at",
                "processed_at",
            )
        )

    ibge_frames[dataset_name] = df_silver
    print(f"IBGE Silver - {dataset_name}: {df_silver.count()} registros após limpeza")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Salvar na camada Silver (Delta Lake)

# COMMAND ----------

if bcb_frames:
    (
        df_bcb_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("serie")
        .save(f"{base_path}/silver/bcb_indicadores")
    )
    print("BCB Silver salvo com sucesso")

for dataset_name, df in ibge_frames.items():
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{base_path}/silver/ibge_{dataset_name}")
    )
    print(f"IBGE Silver ({dataset_name}) salvo com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Registrar tabelas no catálogo

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS brasil_pipeline")

if bcb_frames:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS brasil_pipeline.silver_bcb_indicadores
        USING DELTA
        LOCATION '{base_path}/silver/bcb_indicadores'
    """)

for dataset_name in ibge_frames:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS brasil_pipeline.silver_ibge_{dataset_name}
        USING DELTA
        LOCATION '{base_path}/silver/ibge_{dataset_name}'
    """)

print("Tabelas registradas no catálogo com sucesso")
