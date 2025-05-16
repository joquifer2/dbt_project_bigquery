{{ config(materialized='view') }}

-- =============================================================================
-- Modelo: stg_deals_history
-- Descripción: Vista staging que estandariza y selecciona los campos clave del historial de etapas de deals en Bitrix CRM.
-- Incluye el ID del deal, ID del funnel, fecha de cambio de etapa y el ID de la etapa histórica.
-- =============================================================================

WITH source AS (
  SELECT *
  FROM {{ source('raw_bitrix_deals_history', 'bitrix24crm_deals_stage_history') }}
),

renamed AS (
  SELECT
    CAST(deal_id AS STRING) AS deal_id,                 -- ID único del deal (string)
    CAST(pipeline_id AS STRING) AS funnel_id,           -- ID del funnel (string)
    EXTRACT(DATE FROM created_time) AS created_time_stage_id, -- Fecha de cambio de etapa (date)
    stage_id AS stage_id_history                        -- ID de la etapa histórica
  FROM source
)

SELECT
  deal_id,                  -- ID único del deal
  funnel_id,                -- ID del funnel
  created_time_stage_id,    -- Fecha de cambio de etapa
  stage_id_history          -- ID de la etapa histórica
FROM renamed
