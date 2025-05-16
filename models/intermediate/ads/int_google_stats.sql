{{ config(
    materialized = 'table',
    alias       = 'int_google_stats'
) }}

--------------------------------------------------------------------------------
-- Granularidad final deseada: campaign_id + date (una sola fila por campaña y día)
--------------------------------------------------------------------------------
SELECT
  date,
  campaign_id,
  -- como ya vienen limpios, usamos ANY_VALUE() para no ponerlos en GROUP BY
  ANY_VALUE(campaign_name)   AS campaign_name,
  ANY_VALUE(channel)   AS channel,
  SUM(clicks)                AS clicks,
  SUM(conversions)           AS conversions,
  SUM(impressions)           AS impressions,
  SUM(cost_stats)            AS cost_stats
FROM {{ ref('stg_google_stats') }}
GROUP BY
  date,
  campaign_id

