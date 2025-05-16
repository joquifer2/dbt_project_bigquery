WITH campaign_stats AS (
  SELECT
    CAST(segments_date AS DATE) AS date,
    CAST(campaign_id AS STRING) AS campaign_id,
    metrics_clicks AS clicks,
    metrics_conversions AS conversions,
    CAST(metrics_cost_micros AS FLOAT64) / 1000000 AS cost_stats,
    metrics_impressions AS impressions
  FROM `datamart-393217.google_ads.p_ads_CampaignStats_3082951860`
),

campaign_info AS (
  SELECT
    campaign_id AS raw_campaign_id,
    campaign_name,
    INITCAP(LOWER (campaign_advertising_channel_type)) AS channel,
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY campaign_name) AS row_num
  FROM `datamart-393217.google_ads.p_ads_Campaign_3082951860`
)

SELECT
  cs.date,
  cs.campaign_id,
  cs.clicks,
  cs.conversions,
  cs.cost_stats,
  cs.impressions,
  ci.campaign_name,
  ci.campaign_type
FROM campaign_stats cs
INNER JOIN campaign_info ci
  ON cs.campaign_id = CAST(ci.raw_campaign_id AS STRING)
WHERE ci.row_num = 1

