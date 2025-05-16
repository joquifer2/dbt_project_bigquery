{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_leads_cualificados',
    partition_by={'field': 'date_create_contact', 'data_type': 'date'},
    cluster_by=['region', 'channel'],
    meta={'description': 'Tabla de hechos que registra todos los leads cualificados, con atributos temporales y de dimensión.'}
) }}

-- =============================================================================
-- Modelo: fct_leads_cualificados
-- Descripción: Tabla de hechos que registra todos los leads cualificados, enriquecida con dimensiones temporales, geográficas, comerciales y de canal.
-- Incluye métricas y atributos relevantes para análisis de leads cualificados.
-- =============================================================================

WITH
  -- Leads cualificados base
  leads AS (
    SELECT
      contact_id,            -- ID único del contacto
      date_create_contact,   -- Fecha de creación del contacto
      postal_code,           -- Código postal del contacto
      deal_id,               -- ID único del deal
      date_create_deal,      -- Fecha de creación del deal
      date_closed_lead,      -- Fecha de cierre del lead
      comercial_id,          -- ID del comercial
      funnel_id,             -- ID del funnel
      stage_id,              -- ID de la etapa
      id_marketing_ghl,      -- ID de marketing GHL
      campaign_name,         -- Nombre de la campaña
      campaign_id,           -- ID de la campaña
      canal_ghl,             -- Canal GHL
      source_id,             -- ID de la fuente
      total_amount           -- Monto total del deal
    FROM {{ ref('int_leads_cualificados') }}
  ),

  -- Dimensión temporal: fechas de creación del contacto
  fechas AS (
    SELECT
      date       AS date_create_contact,        -- Fecha de creación del contacto
      date_month AS date_month_create_contact,  -- Mes de creación del contacto
      date_week  AS date_week_create_contact    -- Semana de creación del contacto
    FROM {{ ref('dim_dates') }}
  ),

  -- Dimensión geográfica
  geo AS (
    SELECT
      postal_code,                 -- Código postal
      ANY_VALUE(region) AS region  -- Región asociada al código postal
    FROM {{ ref('dim_geo') }}
    GROUP BY postal_code
  ),

  -- Dimensión de canal
  canal AS (
    SELECT
      source_id,                           -- ID de la fuente
      ANY_VALUE(channel_name) AS channel,   -- Canal de adquisición
      ANY_VALUE(source)       AS source,    -- Fuente
      ANY_VALUE(ads_platform) AS ads_platform, -- Plataforma de anuncios
      ANY_VALUE(medium)       AS medium     -- Medio
    FROM {{ ref('dim_channel') }}
    GROUP BY source_id
  ),

  -- Dimensión de etapa del funnel
  stage AS (
    SELECT
      stage_id,                        -- ID de la etapa
      ANY_VALUE(stage_name) AS stage_name -- Nombre de la etapa
    FROM {{ ref('dim_stage_funnel') }}
    GROUP BY stage_id
  ),

  -- Dimensión comercial
  comercial AS (
    SELECT
      comercial_id,                        -- ID del comercial
      ANY_VALUE(comercial_name) AS comercial_name -- Nombre del comercial
    FROM {{ ref('dim_comercial') }}
    GROUP BY comercial_id
  )

-- =============================================================================
-- SELECT final: une los leads cualificados con todas las dimensiones y fechas
-- =============================================================================
SELECT
  l.contact_id,                        -- ID único del contacto
  l.date_create_contact,                -- Fecha de creación del contacto
  f.date_month_create_contact,          -- Mes de creación del contacto
  f.date_week_create_contact,           -- Semana de creación del contacto
  l.postal_code,                        -- Código postal
  g.region,                             -- Región geográfica
  l.deal_id,                            -- ID único del deal
  l.date_create_deal,                   -- Fecha de creación del deal
  l.date_closed_lead,                   -- Fecha de cierre del lead
  l.comercial_id,                       -- ID del comercial
  c.comercial_name,                     -- Nombre del comercial
  l.funnel_id,                          -- ID del funnel
  l.stage_id,                           -- ID de la etapa
  s.stage_name,                         -- Nombre de la etapa
  l.id_marketing_ghl,                   -- ID de marketing GHL
  l.campaign_name,                      -- Nombre de la campaña
  l.campaign_id,                        -- ID de la campaña
  l.canal_ghl,                          -- Canal GHL
  l.source_id,                          -- ID de la fuente
  cn.channel,                           -- Canal de adquisición
  cn.source,                            -- Fuente
  cn.ads_platform,                      -- Plataforma de anuncios
  cn.medium,                            -- Medio
  l.total_amount                        -- Monto total del deal
FROM leads l
LEFT JOIN fechas     f  ON l.date_create_contact = f.date_create_contact
LEFT JOIN geo        g  ON l.postal_code = g.postal_code
LEFT JOIN canal      cn ON l.source_id   = cn.source_id
LEFT JOIN stage      s  ON l.stage_id    = s.stage_id
LEFT JOIN comercial  c  ON l.comercial_id = c.comercial_id
