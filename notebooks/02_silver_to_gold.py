# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold
# MAGIC Agregações de negócio e métricas analíticas a partir dos dados limpos.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

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
# MAGIC ## 1. Gold: Indicadores Econômicos Consolidados
# MAGIC Pivota as séries do BCB em colunas para facilitar análise cruzada.

# COMMAND ----------

df_bcb = spark.read.format("delta").load(f"{base_path}/silver/bcb_indicadores")

df_indicadores = (
    df_bcb
    .groupBy("data_referencia")
    .pivot("serie", ["selic", "ipca", "cambio_usd_brl", "cdi"])
    .agg(F.first("valor"))
    .orderBy("data_referencia")
)

window_30d = Window.orderBy("data_referencia").rowsBetween(-29, 0)
window_7d = Window.orderBy("data_referencia").rowsBetween(-6, 0)

df_gold_indicadores = (
    df_indicadores
    .withColumn("selic_media_30d", F.avg("selic").over(window_30d))
    .withColumn("cambio_media_7d", F.avg("cambio_usd_brl").over(window_7d))
    .withColumn("cambio_variacao_pct", F.round(
        (F.col("cambio_usd_brl") - F.lag("cambio_usd_brl", 1).over(Window.orderBy("data_referencia")))
        / F.lag("cambio_usd_brl", 1).over(Window.orderBy("data_referencia")) * 100,
        4
    ))
    .withColumn("processed_at", F.current_timestamp())
)

df_gold_indicadores.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold: Resumo Mensal de Indicadores

# COMMAND ----------

df_gold_mensal = (
    df_bcb
    .withColumn("ano_mes", F.date_format("data_referencia", "yyyy-MM"))
    .groupBy("ano_mes", "serie")
    .agg(
        F.avg("valor").alias("media"),
        F.min("valor").alias("minimo"),
        F.max("valor").alias("maximo"),
        F.stddev("valor").alias("desvio_padrao"),
        F.count("valor").alias("qtd_registros"),
    )
    .withColumn("processed_at", F.current_timestamp())
    .orderBy("ano_mes", "serie")
)

df_gold_mensal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: Dados IBGE Agregados

# COMMAND ----------

df_ibge_ipca = spark.read.format("delta").load(f"{base_path}/silver/ibge_ipca_geral")

df_gold_ibge = (
    df_ibge_ipca
    .filter(F.col("valor").isNotNull())
    .withColumn("ano_mes", F.col("periodo"))
    .groupBy("ano_mes", "variavel_nome")
    .agg(
        F.avg("valor").alias("valor_medio"),
        F.count("*").alias("qtd_registros"),
    )
    .withColumn("processed_at", F.current_timestamp())
    .orderBy("ano_mes")
)

df_gold_ibge.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvar na camada Gold (Delta Lake)

# COMMAND ----------

(
    df_gold_indicadores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base_path}/gold/indicadores_consolidados")
)

(
    df_gold_mensal.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("serie")
    .save(f"{base_path}/gold/resumo_mensal")
)

(
    df_gold_ibge.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base_path}/gold/ibge_agregado")
)

print("Tabelas Gold salvas com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Registrar tabelas Gold no catálogo

# COMMAND ----------

gold_tables = {
    "gold_indicadores_consolidados": f"{base_path}/gold/indicadores_consolidados",
    "gold_resumo_mensal": f"{base_path}/gold/resumo_mensal",
    "gold_ibge_agregado": f"{base_path}/gold/ibge_agregado",
}

for table_name, location in gold_tables.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS brasil_pipeline.{table_name}
        USING DELTA
        LOCATION '{location}'
    """)

print("Tabelas Gold registradas no catálogo")
