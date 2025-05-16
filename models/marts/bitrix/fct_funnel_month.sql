{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_funnel_month',
    partition_by={'field': 'month', 'data_type': 'date'},
    meta={'description': 'Embudo mensual consolidado: contactos, leads cualificados, visitas, ventas y coste publicitario, por fecha de creación de contacto.'}
) }}

-- =============================================================================
-- Modelo: fct_funnel_month
-- Descripción: Embudo mensual consolidado con métricas de contactos, leads cualificados, visitas agendadas, visitas efectivas, deals ganados y coste publicitario,
-- referenciados al mes de creación del contacto.
-- =============================================================================

WITH
  -- Total de contactos creados por mes de creación del contacto
  total_contacts AS (
    SELECT
      date_month AS month,                 -- Mes de creación del contacto
      COUNT(*) AS total_contacts           -- Total de contactos creados
    FROM {{ ref('fct_leads') }}
    GROUP BY date_month
  ),

  -- Total de leads cualificados por mes de creación del contacto
  leads_cualificados AS (
    SELECT
      date_month_create_contact AS month,  -- Mes de creación del contacto cualificado
      COUNT(*) AS total_leads_cualificados -- Total de leads cualificados
    FROM {{ ref('fct_leads_cualificados') }}
    GROUP BY date_month_create_contact
  ),

  -- Total de visitas agendadas por mes de creación del contacto
  visitas_agendadas AS (
    SELECT
      date_month_create_contact AS month,      -- Mes de creación del contacto para la visita agendada
      COUNT(*) AS total_visitas_agendadas      -- Total de visitas agendadas
    FROM {{ ref('fct_visitas_agendadas') }}
    GROUP BY date_month_create_contact
  ),

  -- Total de visitas efectivas por mes de creación del contacto
  visitas_efectivas AS (
    SELECT
      date_month_create_contact AS month,      -- Mes de creación del contacto para la visita efectiva
      COUNT(*) AS total_visitas_efectivas      -- Total de visitas efectivas
    FROM {{ ref('fct_visitas_efectivas') }}
    GROUP BY date_month_create_contact
  ),

  -- Total de deals ganados y monto total por mes de creación del contacto
  deals_ganados AS (
    SELECT
      date_month_create_contact AS month,      -- Mes de creación del contacto para el deal ganado
      COUNT(*) AS total_deals_ganados,         -- Total de deals ganados
      SUM(total_amount) AS total_amount        -- Suma de montos de deals ganados
    FROM {{ ref('fct_deals_ganados') }}
    GROUP BY date_month_create_contact
  ),

  -- Coste total mensual de campañas publicitarias (por mes de creación del contacto)
  monthly_costs AS (
    SELECT
      date_month AS month,                 -- Mes de la inversión publicitaria
      SUM(total_cost) AS total_cost        -- Suma de costes publicitarios
    FROM {{ ref('fct_ads_cost_channel') }}
    GROUP BY date_month
  )

-- =============================================================================
-- SELECT final: une todas las métricas del embudo por mes de creación de contacto
-- =============================================================================
SELECT
  tc.month,                                    -- Mes de referencia (creación del contacto)
  COALESCE(tc.total_contacts, 0)              AS total_contacts,           -- Total de contactos
  COALESCE(lc.total_leads_cualificados, 0)    AS total_leads_cualificados,-- Total de leads cualificados
  COALESCE(va.total_visitas_agendadas, 0)     AS total_visitas_agendadas, -- Total de visitas agendadas
  COALESCE(ve.total_visitas_efectivas, 0)     AS total_visitas_efectivas, -- Total de visitas efectivas
  COALESCE(dg.total_deals_ganados, 0)         AS total_deals_ganados,     -- Total de deals ganados
  COALESCE(dg.total_amount, 0)                AS total_amount,            -- Suma de montos de deals ganados
  COALESCE(mc.total_cost, 0)                  AS total_cost               -- Suma de costes publicitarios
FROM total_contacts tc
LEFT JOIN leads_cualificados lc ON tc.month = lc.month
LEFT JOIN visitas_agendadas   va ON tc.month = va.month
LEFT JOIN visitas_efectivas   ve ON tc.month = ve.month
LEFT JOIN deals_ganados       dg ON tc.month = dg.month
LEFT JOIN monthly_costs       mc ON tc.month = mc.month


