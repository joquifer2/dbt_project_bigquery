-- models/intermediate/meta/int_meta2.sql
{{ config(
    materialized='table',
    alias='int_meta2',
    meta={'description': 'Intermedia de métricas de Meta, basada en la vista stg_meta2.'}
) }}

------------------------------------------------------------------------
-- Modelo intermedio: int_meta2
-- Toma la vista stg_meta2 y estandariza nombres de columna
------------------------------------------------------------------------

SELECT
  date                     AS date,          -- fecha de la métrica
  campaign_id              AS campaign_id,   -- ID de campaña en Meta
  impressions              AS impressions,   -- total de impresiones
  clicks                    AS clicks,       -- total de clics
  cost                      AS cost,         -- coste en moneda estándar
  reach                     AS reach,        -- alcance
  conversions               AS conversions,  -- número de conversions
  region_raw               AS region,        -- región tal cual viene en stg_meta2
  channel                 AS channel        -- canal (siempre "Social_Ads")
FROM {{ ref('stg_meta2') }}

