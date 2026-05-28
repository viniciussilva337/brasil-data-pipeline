# Databricks notebook source
# MAGIC %md
# MAGIC # Forecasting: Câmbio USD/BRL
# MAGIC Modelo de séries temporais para previsão do câmbio utilizando dados da camada Gold.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

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
# MAGIC ## 1. Carregar dados da camada Gold

# COMMAND ----------

df_gold = (
    spark.read.format("delta")
    .load(f"{base_path}/gold/indicadores_consolidados")
    .filter(F.col("cambio_usd_brl").isNotNull())
    .orderBy("data_referencia")
)

print(f"Registros carregados: {df_gold.count()}")
df_gold.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND ----------

df_features = (
    df_gold
    .withColumn("dia_semana", F.dayofweek("data_referencia"))
    .withColumn("dia_mes", F.dayofmonth("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("cambio_lag_1", F.lag("cambio_usd_brl", 1).over(
        Window.orderBy("data_referencia")
    ))
    .withColumn("cambio_lag_7", F.lag("cambio_usd_brl", 7).over(
        Window.orderBy("data_referencia")
    ))
    .withColumn("cambio_lag_30", F.lag("cambio_usd_brl", 30).over(
        Window.orderBy("data_referencia")
    ))
    .withColumn("retorno_diario",
        (F.col("cambio_usd_brl") - F.col("cambio_lag_1")) / F.col("cambio_lag_1")
    )
    .withColumn("volatilidade_7d", F.stddev("cambio_usd_brl").over(
        Window.orderBy("data_referencia").rowsBetween(-6, 0)
    ))
    .filter(F.col("cambio_lag_30").isNotNull())
)

print(f"Registros com features: {df_features.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar dados para treino

# COMMAND ----------

pdf = df_features.select(
    "data_referencia",
    "cambio_usd_brl",
    "selic",
    "cdi",
    "dia_semana",
    "dia_mes",
    "mes",
    "cambio_lag_1",
    "cambio_lag_7",
    "cambio_lag_30",
    "retorno_diario",
    "volatilidade_7d",
    "cambio_media_7d",
).toPandas()

pdf = pdf.sort_values("data_referencia").reset_index(drop=True)
pdf = pdf.fillna(method="ffill").dropna()

feature_cols = [
    "selic", "cdi", "dia_semana", "dia_mes", "mes",
    "cambio_lag_1", "cambio_lag_7", "cambio_lag_30",
    "retorno_diario", "volatilidade_7d", "cambio_media_7d",
]

X = pdf[feature_cols].values
y = pdf["cambio_usd_brl"].values

print(f"Shape X: {X.shape}, Shape y: {y.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinar modelo com validação temporal

# COMMAND ----------

tscv = TimeSeriesSplit(n_splits=5)
metrics = {"mae": [], "rmse": [], "r2": []}

for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    metrics["mae"].append(mae)
    metrics["rmse"].append(rmse)
    metrics["r2"].append(r2)

    print(f"Fold {fold+1}: MAE={mae:.4f} | RMSE={rmse:.4f} | R²={r2:.4f}")

print(f"\nMédia: MAE={np.mean(metrics['mae']):.4f} | RMSE={np.mean(metrics['rmse']):.4f} | R²={np.mean(metrics['r2']):.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Modelo final e importância das features

# COMMAND ----------

final_model = GradientBoostingRegressor(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    random_state=42,
)
final_model.fit(X, y)

importances = pd.DataFrame({
    "feature": feature_cols,
    "importance": final_model.feature_importances_,
}).sort_values("importance", ascending=True)

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(importances["feature"], importances["importance"])
ax.set_xlabel("Importância")
ax.set_title("Importância das Features — Previsão Câmbio USD/BRL")
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Previsão vs Real (últimos 60 dias)

# COMMAND ----------

n_plot = min(60, len(pdf))
pdf_plot = pdf.tail(n_plot).copy()
pdf_plot["previsao"] = final_model.predict(pdf_plot[feature_cols].values)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(pdf_plot["data_referencia"], pdf_plot["cambio_usd_brl"], label="Real", linewidth=2)
ax.plot(pdf_plot["data_referencia"], pdf_plot["previsao"], label="Previsão", linewidth=2, linestyle="--")
ax.fill_between(
    pdf_plot["data_referencia"],
    pdf_plot["previsao"] * 0.99,
    pdf_plot["previsao"] * 1.01,
    alpha=0.2, label="Banda ±1%"
)
ax.set_xlabel("Data")
ax.set_ylabel("USD/BRL")
ax.set_title("Câmbio USD/BRL — Real vs Previsão")
ax.legend()
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvar métricas na camada Gold

# COMMAND ----------

metrics_df = spark.createDataFrame([{
    "modelo": "GradientBoostingRegressor",
    "mae": float(np.mean(metrics["mae"])),
    "rmse": float(np.mean(metrics["rmse"])),
    "r2": float(np.mean(metrics["r2"])),
    "n_features": len(feature_cols),
    "n_samples": len(X),
    "n_folds": 5,
    "features": ", ".join(feature_cols),
    "timestamp": pd.Timestamp.now().isoformat(),
}])

(
    metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{base_path}/gold/ml_metrics")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS brasil_pipeline.gold_ml_metrics
    USING DELTA
    LOCATION '{base_path}/gold/ml_metrics'
""")

print("Métricas do modelo salvas em gold/ml_metrics")
