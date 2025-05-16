{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_deals_ganados',
    partition_by={'field': 'date_closed_deal', 'data_type': 'date'},
    cluster_by=['region', 'channel'],
    meta={'description': 'Tabla de hechos que registra todos los deals ganados, con atributos temporales y de dimensión.'}
) }}

-- =============================================================================
-- Modelo: fct_deals_ganados
-- Descripción: Tabla de hechos que registra todos los deals ganados, enriquecida con dimensiones temporales, geográficas, comerciales y de canal.
-- Incluye métricas y atributos relevantes para análisis de ventas ganadas.
-- =============================================================================

WITH deals AS (
    SELECT
        date_closed_deal,      -- Fecha de cierre del deal (ganado)
        contact_id,            -- ID del contacto asociado
        deal_id,               -- ID único del deal
        date_create_contact,   -- Fecha de creación del contacto
        date_create_deal,      -- Fecha de creación del deal
        date_first_visit,      -- Fecha de la primera visita
        comercial_id,          -- ID del comercial
        funnel_id,             -- ID del funnel
        stage_id,              -- ID de la etapa (debe ser 'WON')
        id_marketing_ghl,      -- ID de marketing GHL
        campaign_name,         -- Nombre de la campaña
        campaign_id,           -- ID de la campaña
        canal_ghl,             -- Canal GHL
        source_id,             -- ID de la fuente
        postal_code,           -- Código postal del contacto
        total_amount           -- Monto total del deal
    FROM {{ ref('int_deals_ganados') }}
),

-- Dimensión temporal: fechas de cierre del deal
fechas AS (
    SELECT
        date             AS date_closed_deal,   -- Fecha de cierre del deal
        date_week,                              -- Semana correspondiente
        date_month                              -- Mes correspondiente
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
-- SELECT final: une los deals ganados con todas las dimensiones y fechas
-- =============================================================================
SELECT
    d.date_closed_deal,                -- Fecha de cierre del deal
    f.date_week,                       -- Semana de cierre
    f.date_month,                      -- Mes de cierre
    d.contact_id,                      -- ID del contacto
    d.date_create_contact,             -- Fecha de creación del contacto
    fc.date_month_create_contact,      -- Mes de creación del contacto
    fc.date_week_create_contact,       -- Semana de creación del contacto
    d.postal_code,                     -- Código postal
    g.region,                          -- Región geográfica
    d.deal_id,                         -- ID del deal
    d.date_create_deal,                -- Fecha de creación del deal
    d.date_first_visit,                -- Fecha de la primera visita
    d.comercial_id,                    -- ID del comercial
    c.comercial_name,                  -- Nombre del comercial
    d.funnel_id,                       -- ID del funnel
    d.stage_id,                        -- ID de la etapa
    s.stage_name,                      -- Nombre de la etapa
    d.id_marketing_ghl,                -- ID de marketing GHL
    d.campaign_name,                   -- Nombre de la campaña
    d.campaign_id,                     -- ID de la campaña
    d.canal_ghl,                       -- Canal GHL
    d.source_id,                       -- ID de la fuente
    cn.channel,                        -- Canal de adquisición
    cn.source,                         -- Fuente
    cn.ads_platform,                   -- Plataforma de anuncios
    cn.medium,                         -- Medio
    d.total_amount                     -- Monto total del deal
FROM deals d
LEFT JOIN fechas f
  ON d.date_closed_deal = f.date_closed_deal
LEFT JOIN fechas_contact fc
  ON d.date_create_contact = fc.date_create_contact
LEFT JOIN geo       g ON d.postal_code = g.postal_code
LEFT JOIN canal     cn ON d.source_id    = cn.source_id
LEFT JOIN stage     s ON d.stage_id      = s.stage_id
LEFT JOIN comercial c ON d.comercial_id  = c.comercial_id