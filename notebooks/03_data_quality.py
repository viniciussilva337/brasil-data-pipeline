# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC Validações de completude, unicidade e consistência das camadas Silver e Gold.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
sas_token = dbutils.secrets.get(scope="azure-storage", key="sas-token")

spark.conf.set(
    f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net",
    sas_token,
)

base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Framework de validação

# COMMAND ----------

class QualityCheck:
    def __init__(self):
        self.results = []

    def check_not_null(self, df, column, table_name):
        total = df.count()
        nulls = df.filter(F.col(column).isNull()).count()
        passed = nulls == 0
        self.results.append({
            "table": table_name,
            "check": f"not_null({column})",
            "total_rows": total,
            "failed_rows": nulls,
            "passed": passed,
            "timestamp": datetime.now().isoformat(),
        })
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {table_name}.{column} NOT NULL — {nulls}/{total} nulos")

    def check_unique(self, df, columns, table_name):
        total = df.count()
        distinct = df.select(columns).distinct().count()
        duplicates = total - distinct
        passed = duplicates == 0
        cols_str = ", ".join(columns) if isinstance(columns, list) else columns
        self.results.append({
            "table": table_name,
            "check": f"unique({cols_str})",
            "total_rows": total,
            "failed_rows": duplicates,
            "passed": passed,
            "timestamp": datetime.now().isoformat(),
        })
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {table_name}.({cols_str}) UNIQUE — {duplicates} duplicatas")

    def check_range(self, df, column, min_val, max_val, table_name):
        total = df.count()
        out_of_range = df.filter(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()
        passed = out_of_range == 0
        self.results.append({
            "table": table_name,
            "check": f"range({column}, {min_val}-{max_val})",
            "total_rows": total,
            "failed_rows": out_of_range,
            "passed": passed,
            "timestamp": datetime.now().isoformat(),
        })
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {table_name}.{column} RANGE [{min_val}, {max_val}] — {out_of_range} fora")

    def check_row_count(self, df, min_rows, table_name):
        total = df.count()
        passed = total >= min_rows
        self.results.append({
            "table": table_name,
            "check": f"row_count(>= {min_rows})",
            "total_rows": total,
            "failed_rows": 0 if passed else 1,
            "passed": passed,
            "timestamp": datetime.now().isoformat(),
        })
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {table_name} ROW COUNT — {total} (mínimo: {min_rows})")

    def summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        failed = total - passed
        print(f"\n{'='*60}")
        print(f"RESUMO: {passed}/{total} checks passaram, {failed} falharam")
        print(f"{'='*60}")
        return self.results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar validações — Camada Silver

# COMMAND ----------

qc = QualityCheck()

print("=== Silver: BCB Indicadores ===")
df_bcb = spark.read.format("delta").load(f"{base_path}/silver/bcb_indicadores")
qc.check_row_count(df_bcb, 10, "silver_bcb")
qc.check_not_null(df_bcb, "serie", "silver_bcb")
qc.check_not_null(df_bcb, "valor", "silver_bcb")
qc.check_not_null(df_bcb, "data_referencia", "silver_bcb")
qc.check_unique(df_bcb, ["serie", "data_referencia"], "silver_bcb")
qc.check_range(df_bcb, "valor", 0, 100000, "silver_bcb")

print("\n=== Silver: IBGE Estados ===")
df_estados = spark.read.format("delta").load(f"{base_path}/silver/ibge_estados")
qc.check_row_count(df_estados, 27, "silver_ibge_estados")
qc.check_not_null(df_estados, "sigla", "silver_ibge_estados")
qc.check_unique(df_estados, ["id"], "silver_ibge_estados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar validações — Camada Gold

# COMMAND ----------

print("=== Gold: Indicadores Consolidados ===")
df_gold = spark.read.format("delta").load(f"{base_path}/gold/indicadores_consolidados")
qc.check_row_count(df_gold, 10, "gold_indicadores")
qc.check_not_null(df_gold, "data_referencia", "gold_indicadores")
qc.check_unique(df_gold, ["data_referencia"], "gold_indicadores")

print("\n=== Gold: Resumo Mensal ===")
df_mensal = spark.read.format("delta").load(f"{base_path}/gold/resumo_mensal")
qc.check_row_count(df_mensal, 5, "gold_resumo_mensal")
qc.check_not_null(df_mensal, "ano_mes", "gold_resumo_mensal")
qc.check_range(df_mensal, "media", 0, 100000, "gold_resumo_mensal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo final

# COMMAND ----------

results = qc.summary()

df_results = spark.createDataFrame(results)
(
    df_results.write
    .format("delta")
    .mode("overwrite")
    .save(f"{base_path}/gold/data_quality_results")
)

print("\nResultados de qualidade salvos em gold/data_quality_results")
