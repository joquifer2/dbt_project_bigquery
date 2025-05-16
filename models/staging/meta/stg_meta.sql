{{ config(
    materialized='view',
    alias='stg_meta'
) }}

WITH base_raw AS (

    SELECT
        CAST(metric_date AS DATE)                      AS date,
        CAST(campaign_id AS STRING)                    AS campaign_id,
        CAST(spend AS FLOAT64)                         AS cost,
        COALESCE(CAST(inline_link_clicks AS INT64), 0)  AS clicks,
        COALESCE(CAST(impressions    AS INT64), 0)      AS impressions,
        CAST(campaign_name        AS STRING)           AS campaign_name,
        COALESCE(CAST(reach        AS INT64), 0)       AS reach,
        CAST(objective            AS STRING)           AS objective,
        "Social_ads" AS campaign_type,  -- Añadido el tipo de campaña
        "Social_ads_meta" AS channel,  -- Añadido el canal
        "Meta" AS source_platform,  -- Añadido la plataforma de origen
        COALESCE(CAST(a_onsite_conversion_lead_grouped AS INT64), 0) AS conversions,

        -- limpiamos espacios y convertimos '' a NULL
        NULLIF(TRIM(region), '')                       AS region_raw

    FROM `datamart-393217.raw_meta.facebook_campaign_insights`

),

base AS (

    SELECT
        date,
        campaign_id,
        cost,
        clicks,
        impressions,
        campaign_name,
        reach,
        objective,
        campaign_type,
        channel,
        source_platform,
        conversions,

        -- ahora sí puedo usar region_raw en el CASE
        CASE 
            WHEN region_raw LIKE '%Principality of Asturias%' THEN 'Asturias'
            WHEN region_raw LIKE '%Region of Murcia%'       THEN 'Región de Murcia'
            WHEN region_raw LIKE '%Balearic Islands%'       THEN 'Islas Baleares'
            WHEN region_raw LIKE '%Andalusia%'              THEN 'Andalucía'
            ELSE region_raw
        END AS region,

        -- Añadimos date_week y date_month
        DATE_TRUNC(date, WEEK(MONDAY)) AS date_week,
        DATE_TRUNC(date, MONTH) AS date_month

    FROM base_raw

)

SELECT
    date,
    date_week,  -- Incluimos date_week
    date_month, -- Incluimos date_month
    campaign_id,
    cost,
    clicks,
    impressions,
    campaign_name,
    reach,
    objective,
    conversions,
    region

FROM base

