# Brasil Data Pipeline

Pipeline de engenharia de dados para coleta, processamento e análise de indicadores econômicos brasileiros, utilizando arquitetura **Medallion (Bronze → Silver → Gold)** com **Azure Blob Storage** e **Databricks**.

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ORQUESTRAÇÃO (Databricks Workflow)             │
│                         Execução diária às 06:00 BRT                  │
└──────┬──────────────────┬───────────────────┬──────────────────┬───────┘
       │                  │                   │                  │
       ▼                  ▼                   ▼                  ▼
┌──────────────┐  ┌───────────────┐  ┌────────────────┐  ┌─────────────┐
│  1. INGESTÃO │  │  2. BRONZE →  │  │  3. SILVER →   │  │ 4. QUALITY  │
│              │  │     SILVER    │  │      GOLD      │  │   CHECKS    │
│ Python       │  │ PySpark       │  │ PySpark        │  │ PySpark     │
│ requests     │  │ Delta Lake    │  │ Delta Lake     │  │ Validações  │
│              │  │               │  │                │  │             │
│ APIs:        │  │ • Tipagem     │  │ • Pivot séries │  │ • Not null  │
│ • BCB (SGS)  │  │ • Limpeza     │  │ • Médias 7/30d │  │ • Unicidade │
│ • IBGE       │  │ • Dedup       │  │ • Variação %   │  │ • Ranges    │
│              │  │ • Partição    │  │ • Resumo mensal│  │ • Row count │
└──────┬───────┘  └───────────────┘  └────────┬───────┘  └─────────────┘
       │                                      │
       ▼                                      ▼
┌──────────────┐                     ┌────────────────┐
│ AZURE BLOB   │                     │   ML MODULE    │
│ STORAGE      │                     │                │
│              │                     │ • Features lag │
│ bronze/      │                     │ • GBRegressor  │
│  ├── bcb/    │                     │ • TimeSeriesCV │
│  └── ibge/   │                     │ • Previsão FX  │
│ silver/      │                     └────────────────┘
│  ├── bcb_*   │
│  └── ibge_*  │
│ gold/        │
│  ├── indicadores_consolidados      ┌────────────────┐
│  ├── resumo_mensal                 │   SQL QUERIES  │
│  ├── ibge_agregado                 │                │
│  └── ml_metrics                    │ Análises:      │
└──────────────┘                     │ • Evolução     │
                                     │ • Comparativos │
                                     │ • Correlações  │
                                     └────────────────┘
```

## Fontes de Dados

| Fonte | API | Dados coletados |
|-------|-----|-----------------|
| **Banco Central (BCB)** | SGS - Sistema Gerenciador de Séries | SELIC, IPCA, Câmbio USD/BRL, CDI |
| **IBGE** | API de Agregados | PIB, IPCA Geral, População, Estados |

## Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Ingestão | Python, Requests |
| Armazenamento | Azure Blob Storage |
| Processamento | PySpark, Delta Lake |
| Orquestração | Databricks Workflows |
| Machine Learning | scikit-learn (GradientBoosting) |
| Consultas analíticas | SQL (Databricks SQL) |
| Conteinerização | Docker, Docker Compose |
| Testes | pytest (22 testes) |

## Estrutura do Projeto

```
brasil-data-pipeline/
├── config/
│   └── settings.py              # Configurações centralizadas (URLs, séries, params)
├── databricks/
│   └── workflow.json            # DAG de orquestração do Databricks
├── notebooks/
│   ├── 01_bronze_to_silver.py   # Limpeza e padronização
│   ├── 02_silver_to_gold.py     # Agregações de negócio
│   ├── 03_data_quality.py       # Validações automatizadas
│   └── 04_ml_forecasting.py     # Modelo de previsão de câmbio
├── sql/
│   └── gold_queries.sql         # Queries analíticas para a camada Gold
├── src/
│   ├── ingestion/
│   │   ├── bcb_client.py        # Cliente API do Banco Central
│   │   ├── ibge_client.py       # Cliente API do IBGE
│   │   └── upload_to_azure.py   # Upload para Azure Blob Storage
│   ├── utils/
│   │   └── logger.py            # Sistema de logging
│   └── main.py                  # Orquestrador da ingestão
├── tests/
│   ├── test_bcb_client.py       # Testes do cliente BCB
│   ├── test_ibge_client.py      # Testes do cliente IBGE
│   ├── test_upload_to_azure.py  # Testes do upload Azure
│   └── test_main.py             # Testes do pipeline
├── docker-compose.yml           # Ambiente local com Azurite
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Como Executar

### Pré-requisitos

- Python 3.11+
- Docker e Docker Compose
- Conta Azure com Blob Storage (ou usar Azurite localmente)

### Execução local com Docker

```bash
# Subir o ambiente (Azurite + Pipeline)
docker-compose up --build

# Isso irá:
# 1. Iniciar o Azurite (emulador local do Azure Storage)
# 2. Executar o pipeline de ingestão coletando dados reais das APIs
# 3. Fazer upload dos dados para o Azurite (camada Bronze)
```

### Execução direta com Python

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas credenciais Azure

# Executar pipeline
python src/main.py
```

### Testes

```bash
pytest tests/ -v
```

### Databricks

1. Importar os notebooks de `notebooks/` para o Databricks
2. Configurar os secrets do Azure Storage no scope `azure-storage`
3. Importar o workflow de `databricks/workflow.json`
4. Configurar as variáveis `CLUSTER_ID`, `STORAGE_ACCOUNT` e `ALERT_EMAIL`

## Decisões Técnicas

**Arquitetura Medallion** — Separação em Bronze (dados brutos), Silver (dados limpos) e Gold (dados agregados) para garantir rastreabilidade e reprocessamento.

**Delta Lake** — Formato colunar com suporte a ACID transactions, schema evolution e time travel, ideal para pipelines incrementais.

**Períodos dinâmicos do IBGE** — A API do IBGE não suporta parâmetros relativos (`ultimos6`) para todos os agregados. O cliente busca os períodos disponíveis dinamicamente antes de cada requisição.

**Retry com backoff** — Todas as chamadas às APIs possuem retry automático (3 tentativas com delay de 5s) para lidar com instabilidades.

**TimeSeriesSplit no ML** — Validação cruzada temporal (não aleatória) para evitar data leakage na previsão do câmbio.

## Modelo de Machine Learning

O notebook `04_ml_forecasting.py` treina um **GradientBoostingRegressor** para previsão do câmbio USD/BRL utilizando:

**Features:**
- Indicadores econômicos (SELIC, CDI)
- Lags temporais (1, 7 e 30 dias)
- Retorno diário e volatilidade (7 dias)
- Média móvel (7 dias)
- Sazonalidade (dia da semana, dia do mês, mês)

**Validação:** TimeSeriesSplit com 5 folds, garantindo que o treino sempre precede o teste cronologicamente.
