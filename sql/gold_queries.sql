-- ============================================================
-- Queries analíticas para a camada Gold
-- Executar no Databricks SQL
-- ============================================================

-- 1. Evolução da SELIC nos últimos 12 meses
SELECT
    data_referencia,
    selic,
    selic_media_30d,
    ROUND(selic - LAG(selic, 1) OVER (ORDER BY data_referencia), 4) AS variacao_diaria
FROM brasil_pipeline.gold_indicadores_consolidados
WHERE data_referencia >= ADD_MONTHS(CURRENT_DATE(), -12)
  AND selic IS NOT NULL
ORDER BY data_referencia;


-- 2. Câmbio USD/BRL — máximas e mínimas mensais
SELECT
    ano_mes,
    media AS cambio_medio,
    minimo AS cambio_minimo,
    maximo AS cambio_maximo,
    desvio_padrao AS volatilidade,
    qtd_registros AS dias_uteis
FROM brasil_pipeline.gold_resumo_mensal
WHERE serie = 'cambio_usd_brl'
ORDER BY ano_mes DESC
LIMIT 12;


-- 3. Comparativo SELIC vs CDI vs IPCA (mensal)
SELECT
    s.ano_mes,
    s.media AS selic_media,
    c.media AS cdi_medio,
    i.media AS ipca_medio,
    ROUND(s.media - i.media, 4) AS juros_real
FROM brasil_pipeline.gold_resumo_mensal s
LEFT JOIN brasil_pipeline.gold_resumo_mensal c
    ON s.ano_mes = c.ano_mes AND c.serie = 'cdi'
LEFT JOIN brasil_pipeline.gold_resumo_mensal i
    ON s.ano_mes = i.ano_mes AND i.serie = 'ipca'
WHERE s.serie = 'selic'
ORDER BY s.ano_mes DESC
LIMIT 12;


-- 4. Correlação Câmbio x SELIC (dados diários)
SELECT
    data_referencia,
    selic,
    cambio_usd_brl,
    cambio_variacao_pct
FROM brasil_pipeline.gold_indicadores_consolidados
WHERE selic IS NOT NULL
  AND cambio_usd_brl IS NOT NULL
ORDER BY data_referencia;


-- 5. Resumo geral — último valor disponível de cada indicador
SELECT
    'SELIC' AS indicador, MAX(data_referencia) AS ultima_data,
    FIRST_VALUE(selic) OVER (ORDER BY data_referencia DESC) AS ultimo_valor
FROM brasil_pipeline.gold_indicadores_consolidados
WHERE selic IS NOT NULL

UNION ALL

SELECT
    'Câmbio USD/BRL', MAX(data_referencia),
    FIRST_VALUE(cambio_usd_brl) OVER (ORDER BY data_referencia DESC)
FROM brasil_pipeline.gold_indicadores_consolidados
WHERE cambio_usd_brl IS NOT NULL

UNION ALL

SELECT
    'CDI', MAX(data_referencia),
    FIRST_VALUE(cdi) OVER (ORDER BY data_referencia DESC)
FROM brasil_pipeline.gold_indicadores_consolidados
WHERE cdi IS NOT NULL;


-- 6. IBGE — IPCA por período
SELECT
    ano_mes,
    variavel_nome,
    valor_medio,
    qtd_registros
FROM brasil_pipeline.gold_ibge_agregado
ORDER BY ano_mes DESC;
