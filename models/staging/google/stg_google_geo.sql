{{ config(
    materialized = 'view'
) }}

SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    segments_date AS date,
    REGEXP_EXTRACT(segments_geo_target_most_specific_location, r"geoTargetConstants/(\d+)") AS geo_id_ads,
    metrics_clicks AS clicks,
    metrics_impressions AS impressions,
    CAST(metrics_cost_micros AS FLOAT64) / 1000000 AS cost,
    metrics_conversions AS conversions
FROM `datamart-393217.google_ads.p_ads_GeoStats_3082951860`
