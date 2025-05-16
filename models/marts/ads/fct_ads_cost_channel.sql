{{ config(
    materialized = 'table',
    schema       = 'marts_ads',
    alias        = 'fct_ads_cost_channel',
    partition_by = {'field': 'date', 'data_type': 'date'},
    cluster_by   = ['channel'],
    meta         = {'description': 'Coste publicitario diario por canal (Google, Meta), incluyendo week y month desde dim_dates.'}
) }}

-- =============================================================================
-- Modelo: fct_ads_cost_channel
-- Descripción: Agrega el coste publicitario diario por canal, unificando datos
--              de Google y Meta, e incorpora atributos temporales (semana y mes).
-- Buenas prácticas: documentar cada paso y nombrar claramente las CTEs.
-- =============================================================================

WITH
  -- 1) google_cost: sumarización del coste de Google Ads
  google_cost AS (
    SELECT
      date,                         -- fecha del coste
      channel,                      -- canal de publicidad (p.ej., 'google')
      SUM(cost_stats) AS cost_eur   -- coste total en EUR para Google Ads
    FROM {{ ref('int_google_stats') }}
    GROUP BY date, channel         -- agrupar por fecha y canal
  ),

  -- 2) meta_cost: sumarización del coste de Meta Ads
  meta_cost AS (
    SELECT
      date,                         -- fecha del coste
      channel,                      -- canal de publicidad (p.ej., 'facebook')
      SUM(cost) AS cost_eur         -- coste total en EUR para Meta Ads
    FROM {{ ref('int_meta2') }}
    GROUP BY date, channel         -- agrupar por fecha y canal
  ),

  -- 3) costes_unidos: unión de los costes de Google y Meta
  costes_unidos AS (
    SELECT * FROM google_cost      -- incluir todos los registros de Google
    UNION ALL                      -- usar UNION ALL para mantener duplicados intencionales
    SELECT * FROM meta_cost        -- incluir todos los registros de Meta
  ),

  -- 4) costes_agrupados: coste total diario por canal
  costes_agrupados AS (
    SELECT
      date,                          -- fecha de coste
      channel,                       -- canal de publicidad
      SUM(cost_eur) AS total_cost   -- coste acumulado de ambos proveedores
    FROM costes_unidos              -- datos combinados
    GROUP BY date, channel          -- agrupar por fecha y canal
  ),

  -- 5) fechas: atributos temporales desde la dimensión de fechas
  fechas AS (
    SELECT
      date          AS date,        -- fecha para join
      date_week,                     -- semana del año (p.ej., '2025-W20')
      date_month                     -- mes (primer día del mes)
    FROM {{ ref('dim_dates') }}      -- dimensión de fechas
  )

-- SELECT final: combinar costes con fechas para enriquecer la tabla de hechos
SELECT
  c.date,                          -- fecha del coste
  f.date_week,                     -- semana asociada
  f.date_month,                    -- mes asociado
  c.channel,                       -- canal de publicidad
  c.total_cost                     -- coste diario total
FROM costes_agrupados AS c         -- tabla de costes agregados
LEFT JOIN fechas AS f              -- incorporar atributos temporales
  ON c.date = f.date
-- Ordenar por fecha descendente para análisis de reciente a antiguo
ORDER BY c.date DESC
