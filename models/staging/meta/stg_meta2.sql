{{ config(
    materialized='view',
    alias='stg_meta2'
) }}

SELECT
  CAST(metric_date AS DATE) AS date,
  CAST(campaign_id AS STRING) AS campaign_id,
  COALESCE(CAST(impressions AS INT64), 0) AS impressions,
  COALESCE(CAST(inline_link_clicks AS INT64), 0) AS clicks,
  COALESCE(CAST(spend AS FLOAT64), 0.0) AS cost,
  COALESCE(CAST(reach AS INT64), 0) AS reach,
  COALESCE(CAST(a_onsite_conversion_lead_grouped AS INT64), 0) AS conversions,
  NULLIF(TRIM(region), '') AS region_raw,
  "Social_Ads" AS channel
FROM `datamart-393217.raw_meta.facebook_campaign_insights`
