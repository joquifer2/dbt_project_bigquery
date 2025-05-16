-- models/intermediate/ads/int_google_geo.sql
-- Modelo intermedio: int_google_geo
-- Este modelo toma los datos geográficos de campañas de Google Ads desde stg_google_geo y estandariza los nombres de columna para su uso posterior en el data mart.

{{ config(
    materialized = 'table',
    alias = 'int_google_geo'
) }}

-- Selecciona y renombra las columnas relevantes de la vista stg_google_geo
SELECT
  date,                -- Fecha de la métrica (YYYY-MM-DD)
  campaign_id,         -- ID de la campaña en Google Ads
  geo_id_ads,          -- ID geográfico asociado a la campaña
  clicks,              -- Total de clics
  conversions,         -- Total de conversiones
  impressions,         -- Total de impresiones
  cost AS cost_geo     -- Costo de la campaña en la región
FROM {{ ref('stg_google_geo') }}
