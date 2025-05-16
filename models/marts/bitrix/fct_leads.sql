{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_leads',
    partition_by={'field': 'date_create_contact', 'data_type': 'date'},
    cluster_by=['region', 'channel'],
    meta={'description': 'Tabla de hechos que registra todos los leads generados desde CRM con sus atributos enriquecidos desde dimensiones.'}
) }}

-- =============================================================================
-- Modelo: fct_leads
-- Descripción: Tabla de hechos que registra todos los leads generados desde CRM, enriquecida con dimensiones temporales, geográficas, comerciales y de canal.
-- Incluye métricas y atributos relevantes para análisis de leads.
-- =============================================================================

WITH
  -- Base de leads desde int_contacts
  base AS (
    SELECT 
      * -- Todos los campos del modelo intermedio int_contacts
    FROM {{ ref('int_contacts') }}
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
  channel AS (
    SELECT
      source_id,                           -- ID de la fuente
      ANY_VALUE(channel_name) AS channel_name,   -- Canal de adquisición
      ANY_VALUE(source) AS source,              -- Fuente
      ANY_VALUE(ads_platform) AS ads_platform,  -- Plataforma de anuncios
      ANY_VALUE(medium) AS medium               -- Medio
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

  -- Dimensión temporal: fechas de creación del contacto
  date_dim AS (
    SELECT
      date          AS date_create_contact, -- Fecha de creación del contacto
      date_week,                            -- Semana de creación
      date_month                            -- Mes de creación
    FROM {{ ref('dim_dates') }}
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
-- SELECT final: une los leads con todas las dimensiones y fechas
-- =============================================================================
SELECT
  b.contact_id,                -- ID único del contacto
  b.date_create_contact,       -- Fecha de creación del contacto
  d.date_week,                 -- Semana de creación del contacto
  d.date_month,                -- Mes de creación del contacto
  b.postal_code,               -- Código postal
  g.region,                    -- Región geográfica

  b.deal_id,                   -- ID único del deal
  b.date_create_deal,          -- Fecha de creación del deal
  b.date_closed_lead,          -- Fecha de cierre del lead
  b.date_first_visit,          -- Fecha de la primera visita
  
  b.funnel_id,                 -- ID del funnel
  b.stage_id,                  -- ID de la etapa
  s.stage_name,                -- Nombre de la etapa
  b.comercial_id,              -- ID del comercial
  cm.comercial_name,           -- Nombre del comercial

  b.id_marketing_ghl,          -- ID de marketing GHL
  b.campaign_name,             -- Nombre de la campaña
  b.campaign_id,               -- ID de la campaña
  b.canal_ghl,                 -- Canal GHL
  b.source_id,                 -- ID de la fuente
  c.channel_name AS channel,   -- Canal de adquisición
  c.source,                    -- Fuente
  c.ads_platform,              -- Plataforma de anuncios
  c.medium,                    -- Medio

  b.total_amount,              -- Monto total del deal
  b.is_qualified               -- Flag de cualificación

FROM base b

LEFT JOIN geo g
  ON b.postal_code = g.postal_code
  AND b.postal_code IS NOT NULL
  AND TRIM(b.postal_code) != ''

LEFT JOIN channel c
  ON b.source_id = c.source_id
  AND b.source_id IS NOT NULL
  AND TRIM(b.source_id) != ''

LEFT JOIN stage s
  ON b.stage_id = s.stage_id
  AND b.stage_id IS NOT NULL
  AND TRIM(b.stage_id) != ''

LEFT JOIN date_dim d
  ON b.date_create_contact = d.date_create_contact

LEFT JOIN comercial cm
  ON b.comercial_id = cm.comercial_id
  AND b.comercial_id IS NOT NULL
  AND TRIM(b.comercial_id) != ''