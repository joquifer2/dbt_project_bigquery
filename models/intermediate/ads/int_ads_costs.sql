-- models/intermediate/ads/int_ads_costs.sql
-- Modelo intermedio: int_ads_costs
-- Este modelo agrega los costes diarios de Google Ads y Meta Ads, mostrando cada uno en columnas separadas y calculando el coste total diario.

{{ config(
    materialized = 'table',
    alias        = 'int_ads_costs'
) }}

-- Costes diarios de Google Ads
WITH google_cost AS (
  SELECT
    date,                              -- Fecha de la métrica (YYYY-MM-DD)
    SUM(cost_stats) AS google_cost     -- Suma diaria del coste de Google Ads
  FROM {{ ref('int_google_stats') }}
  GROUP BY date
),

-- Costes diarios de Meta Ads
meta_cost AS (
  SELECT
    date,                             -- Fecha de la métrica (YYYY-MM-DD)
    SUM(cost) AS meta_cost            -- Suma diaria del coste de Meta Ads
  FROM {{ ref('int_meta2') }}
  GROUP BY date
)

-- Unifica los costes diarios de ambas plataformas y calcula el total
SELECT
  COALESCE(g.date, m.date)     AS date,         -- Fecha consolidada
  COALESCE(g.google_cost, 0.0) AS google_cost,  -- Coste Google Ads (0 si no hay dato)
  COALESCE(m.meta_cost, 0.0)   AS meta_cost,    -- Coste Meta Ads (0 si no hay dato)
  (COALESCE(g.google_cost, 0.0) + COALESCE(m.meta_cost, 0.0)) AS total_cost  -- Suma total diaria
FROM google_cost g
FULL OUTER JOIN meta_cost m
  ON g.date = m.date
ORDER BY date





