{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_visitas_efectivas',
    partition_by={'field': 'date_first_visit', 'data_type': 'date'},
    cluster_by=['region', 'channel'],
    meta={'description': 'Tabla de hechos que registra todas las visitas efectivas por comercial, con atributos temporales y de dimensión. Si falta date_first_visit, se usa date_create_deal.'}
) }}

-- =============================================================================
-- Modelo: fct_visitas_efectivas
-- Descripción: Tabla de hechos que registra todas las visitas efectivas por comercial, enriquecida con dimensiones temporales, geográficas, comerciales y de canal.
-- Si falta date_first_visit, se usa date_create_deal.
-- =============================================================================

WITH
  -- Visitas efectivas base
  visitas AS (
    SELECT
      date_first_visit,           -- Fecha de la visita efectiva (o creación del deal si falta)
      contact_id,                 -- ID único del contacto
      date_create_contact,        -- Fecha de creación del contacto
      postal_code,                -- Código postal del contacto
      deal_id,                    -- ID único del deal
      date_create_deal,           -- Fecha de creación del deal
      comercial_id,               -- ID del comercial
      funnel_id,                  -- ID del funnel
      stage_id,                   -- ID de la etapa
      id_marketing_ghl,           -- ID de marketing GHL
      campaign_name,              -- Nombre de la campaña
      campaign_id,                -- ID de la campaña
      canal_ghl,                  -- Canal GHL
      source_id                   -- ID de la fuente
    FROM {{ ref('int_visitas_efectivas') }}
  ),

  -- Dimensión temporal: fechas de la visita efectiva
  fechas AS (
    SELECT
      date             AS date_first_visit,   -- Fecha de la visita efectiva
      date_week,                              -- Semana de la visita
      date_month                              -- Mes de la visita
    FROM {{ ref('dim_dates') }}
  ),

  -- Dimensión temporal: fechas de creación del contacto
  fechas_contact AS (
    SELECT
      date             AS date_create_contact,        -- Fecha de creación del contacto
      date_month       AS date_month_create_contact,  -- Mes de creación del contacto
      date_week        AS date_week_create_contact    -- Semana de creación del contacto
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
-- SELECT final: une las visitas efectivas con todas las dimensiones y fechas
-- =============================================================================
SELECT
  v.date_first_visit,                -- Fecha de la visita efectiva
  f.date_week,                       -- Semana de la visita
  f.date_month,                      -- Mes de la visita
  v.contact_id,                      -- ID único del contacto
  v.date_create_contact,             -- Fecha de creación del contacto
  fc.date_month_create_contact,      -- Mes de creación del contacto
  fc.date_week_create_contact,       -- Semana de creación del contacto
  v.postal_code,                     -- Código postal
  g.region,                          -- Región geográfica
  v.deal_id,                         -- ID único del deal
  v.date_create_deal,                -- Fecha de creación del deal
  v.comercial_id,                    -- ID del comercial
  c.comercial_name,                  -- Nombre del comercial
  v.funnel_id,                       -- ID del funnel
  v.stage_id,                        -- ID de la etapa
  s.stage_name,                      -- Nombre de la etapa
  v.id_marketing_ghl,                -- ID de marketing GHL
  v.campaign_name,                   -- Nombre de la campaña
  v.campaign_id,                     -- ID de la campaña
  v.canal_ghl,                       -- Canal GHL
  v.source_id,                       -- ID de la fuente
  cn.channel,                        -- Canal de adquisición
  cn.source,                         -- Fuente
  cn.ads_platform,                   -- Plataforma de anuncios
  cn.medium                          -- Medio
FROM visitas v
LEFT JOIN fechas f
  ON v.date_first_visit = f.date_first_visit
LEFT JOIN fechas_contact fc
  ON v.date_create_contact = fc.date_create_contact
LEFT JOIN geo       g ON v.postal_code = g.postal_code
LEFT JOIN canal     cn ON v.source_id    = cn.source_id
LEFT JOIN stage     s ON v.stage_id      = s.stage_id
LEFT JOIN comercial c ON v.comercial_id  = c.comercial_id