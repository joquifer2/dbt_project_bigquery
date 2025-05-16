{{ config(materialized='view') }}

-- =============================================================================
-- Modelo: stg_deals
-- Descripción: Vista staging que estandariza y selecciona los campos clave de los deals de Bitrix CRM.
-- Incluye el ID del deal, IDs de comercial y contacto, fechas, canal, fuente, importe y campos custom.
-- =============================================================================

WITH deals AS (
  SELECT
      CAST(id AS STRING) AS deal_id,                 -- ID único del deal (string)
      CAST(assigned_by_id AS STRING) AS comercial_id, -- ID del comercial (string)
      CAST(contact_id AS STRING) AS contact_id,       -- ID del contacto (string)
      CAST(category_id AS STRING) AS funnel_id,       -- ID del funnel (string)
      CAST(stage_id AS STRING) AS stage_id,           -- ID de la etapa (string)
      EXTRACT(DATE FROM date_create) AS date_create_deal, -- Fecha de creación del deal (date)
      EXTRACT(DATE FROM closedate) AS date_closed_lead,   -- Fecha de cierre del lead (date)
      source_id,                                      -- ID de la fuente
      opportunity AS total_amount                     -- Importe total del deal
  FROM `datamart-393217`.`raw_bitrix_deals`.`bitrix24crm_deals`
  -- Filtramos para que no aparezcan los deals de prueba/test
  WHERE NOT REGEXP_CONTAINS(title,"[Tt][Ee][Ss][Tt]|[Pr][Rr][[Uu][Ee][Bb][Aa]")
),

custom_fields AS (
  SELECT
      CAST(deal_id AS STRING) AS deal_id,             -- ID único del deal (string)
      EXTRACT(DATE FROM fecha_visita_) AS date_first_visit, -- Fecha de la primera visita (date)
      CAST(id_marketing_ghl AS STRING) AS id_marketing_ghl, -- ID de marketing GHL (string)
      CAST(nombre_de_campa_a AS STRING) AS campaign_name,   -- Nombre de la campaña (string)
      -- Descomponemos los múltiples campaign_id en varias filas
      SPLIT(CAST(id_campa_a AS STRING), ' ')[OFFSET(0)] AS campaign_id, -- ID de la campaña (string)
      NULLIF(TRIM(canal_ghl), '') AS canal_ghl,         -- Canal GHL (string, limpio)
      CASE
          -- 1) Si viene de Google/AdWords
          WHEN id_marketing_ghl IS NOT NULL
            AND REGEXP_CONTAINS(LOWER(canal_ghl), r'adwords|google')
            THEN 'Google ads'
          -- 2) Si viene de Facebook
          WHEN id_marketing_ghl IS NOT NULL
            AND REGEXP_CONTAINS(LOWER(canal_ghl), r'facebook')
            THEN 'Meta'
          -- 3) Cualquier otro canal, siempre que tenga id_marketing
          WHEN id_marketing_ghl IS NOT NULL
            THEN 'Google ads'
          -- 4) Si no tiene id_marketing, devolvemos NULL
          ELSE NULL
        END AS source_platform
    FROM `datamart-393217`.`raw_bitrix_deals_cfields`.`bitrix24crm_deals_custom_fields_data`
),

final AS (
  SELECT
    d.deal_id,
    d.comercial_id,
    d.contact_id,
    d.funnel_id,
    d.date_create_deal,
    d.date_closed_lead,
    cf.date_first_visit,
    cf.id_marketing_ghl,
    cf.campaign_name,
    cf.campaign_id,
    d.source_id,
    d.stage_id,
    cf.canal_ghl,
    cf.source_platform,
    d.total_amount
  FROM deals d
  LEFT JOIN custom_fields cf USING (deal_id)
)

SELECT
  deal_id,                -- ID único del deal
  comercial_id,           -- ID del comercial
  contact_id,             -- ID del contacto
  funnel_id,              -- ID del funnel
  date_create_deal,       -- Fecha de creación del deal
  date_closed_lead,       -- Fecha de cierre del lead
  date_first_visit,       -- Fecha de la primera visita
  id_marketing_ghl,       -- ID de marketing GHL
  campaign_name,          -- Nombre de la campaña
  campaign_id,            -- ID de la campaña
  source_id,              -- ID de la fuente
  stage_id,               -- ID de la etapa
  canal_ghl,              -- Canal GHL
  source_platform,        -- Plataforma de origen (Google ads, Meta, etc.)
  total_amount            -- Importe total del deal
FROM final