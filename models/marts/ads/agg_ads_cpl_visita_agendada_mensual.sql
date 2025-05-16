{{ config(
    materialized = 'table',
    alias        = 'agg_ads_cpl_visita_agendada_mensual',
    schema       = 'marts_ads'
) }}

-- =============================================================================
-- Modelo: agg_ads_cpl_visita_agendada_mensual
-- Descripción: Calcula el Costo Por Lead (CPL) para visitas agendadas a nivel mensual,
--              dividiendo el coste total publicitario del mes entre el número de visitas
--              agendadas en ese mismo mes.
-- Buena práctica: documentar siempre el propósito del modelo y cada paso de transformación.
-- =============================================================================

WITH

  -- 1) monthly_costs: sumatorio del coste publicitario por mes
  monthly_costs AS (
    SELECT
      date_month,                       -- fecha de referencia (primer día del mes)
      SUM(total_cost) AS total_cost     -- coste total acumulado en el mes
    FROM  {{ ref('fct_ads_cost_channel') }}
    WHERE date_month IS NOT NULL       -- filtrar registros válidos de fecha
    GROUP BY date_month                 -- agrupar por mes
  ),

  -- 2) monthly_visits: conteo de visitas agendadas por mes
  monthly_visits AS (
    SELECT
      date_month,                        -- misma fecha de referencia que en costes
      COUNT(*) AS total_visitas_agendadas  -- número total de visitas agendadas
    FROM {{ ref('fct_visitas_agendadas') }}
    WHERE date_month IS NOT NULL        -- asegurar existencia de la fecha
    GROUP BY date_month                  -- agrupar por mes
  )

-- 3) SELECT final: combinación de costes y visitas para calcular el CPL
SELECT
  c.date_month                                   AS month,                    -- mes de referencia
  c.total_cost,                                                              -- coste total del mes
  v.total_visitas_agendadas,                                                -- visitas agendadas en el mes
  COALESCE(
    SAFE_DIVIDE(c.total_cost, v.total_visitas_agendadas), 
    0
  )                                               AS cpl_visita_agendada     -- CPL: coste dividido por visitas
FROM monthly_costs AS c
LEFT JOIN monthly_visits AS v
  ON c.date_month = v.date_month                  -- unir costes con visitas por mes
ORDER BY c.date_month DESC                        -- ordenar de más reciente a más antiguo
