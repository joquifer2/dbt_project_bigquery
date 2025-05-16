{{ config(
    materialized='table',
    schema='marts_bitrix',
    alias='fct_comercial_month',
    partition_by={'field': 'date_month', 'data_type': 'date'},
    cluster_by=['comercial_name']
) }}

-- =============================================================================
-- Modelo: fct_comercial_month
-- Descripción: Métricas mensuales por comercial (nombre e ID):
--   • total_visitas_agendadas  
--   • total_visitas_efectivas  
--   • total_ganados y total_amount  
--   • cpl_visita_agendada (costo por lead agendado)  
-- =============================================================================

WITH

  --------------------------------------------------------------------------
  -- 1) Base de comerciales + mes: todos los comerciales que tuvieron
  --    actividad en visitas agendadas, efectivas o deals ganados.
  --------------------------------------------------------------------------
  base_comercial_mes AS (
    SELECT DISTINCT comercial_name, comercial_id, date_month
      FROM `datamart-393217.bkm_marts_bitrix.fct_visitas_agendadas`
    UNION DISTINCT
    SELECT DISTINCT comercial_name, comercial_id, date_month
      FROM `datamart-393217.bkm_marts_bitrix.fct_visitas_efectivas`
    UNION DISTINCT
    SELECT DISTINCT comercial_name, comercial_id, date_month
      FROM `datamart-393217.bkm_marts_bitrix.fct_deals_ganados`
  ),

  --------------------------------------------------------------------------
  -- 2) Visitas agendadas por comercial y mes
  --------------------------------------------------------------------------
  visitas_agendadas AS (
    SELECT
      comercial_name,
      comercial_id,
      date_month        AS date_month,
      COUNT(*)          AS total_visitas_agendadas
    FROM `datamart-393217.bkm_marts_bitrix.fct_visitas_agendadas`
    GROUP BY comercial_name, comercial_id, date_month
  ),

  --------------------------------------------------------------------------
  -- 3) Visitas efectivas por comercial y mes
  --------------------------------------------------------------------------
  visitas_efectivas AS (
    SELECT
      comercial_name,
      comercial_id,
      date_month        AS date_month,
      COUNT(*)          AS total_visitas_efectivas
    FROM `datamart-393217.bkm_marts_bitrix.fct_visitas_efectivas`
    GROUP BY comercial_name, comercial_id, date_month
  ),

  --------------------------------------------------------------------------
  -- 4) Deals ganados e importe total por comercial y mes
  --------------------------------------------------------------------------
  deals_ganados AS (
    SELECT
      comercial_name,
      comercial_id,
      date_month        AS date_month,
      COUNT(*)          AS total_ganados,
      SUM(total_amount) AS total_amount
    FROM `datamart-393217.bkm_marts_bitrix.fct_deals_ganados`
    GROUP BY comercial_name, comercial_id, date_month
  ),

  --------------------------------------------------------------------------
  -- 5) CPL por lead agendado: mes y valor CPL
  --------------------------------------------------------------------------
  cost_per_lead AS (
    SELECT
      month          AS date_month,
      cpl_visita_agendada
    FROM `datamart-393217.bkm_marts_ads.agg_ads_cpl_visita_agendada_mensual`
  )

-- =============================================================================
-- SELECT final: unir todas las métricas usando LEFT JOIN sobre la base
-- =============================================================================
SELECT
  b.comercial_name,                                -- nombre del comercial
  b.comercial_id,                                  -- ID del comercial (siempre NOT NULL)
  b.date_month,                                    -- mes de referencia
  COALESCE(va.total_visitas_agendadas, 0)   AS total_visitas_agendadas,
  COALESCE(ve.total_visitas_efectivas, 0)   AS total_visitas_efectivas,
  COALESCE(dg.total_ganados, 0)             AS total_ganados,
  COALESCE(dg.total_amount, 0)             AS total_amount,
  COALESCE(cpl.cpl_visita_agendada, 0)     AS cpl_visita_agendada

FROM base_comercial_mes b

-- Aseguramos que cada métrica se asocia al mismo comercial y mes
LEFT JOIN visitas_agendadas va
  ON b.comercial_id = va.comercial_id
 AND b.date_month    = va.date_month

LEFT JOIN visitas_efectivas ve
  ON b.comercial_id = ve.comercial_id
 AND b.date_month    = ve.date_month

LEFT JOIN deals_ganados dg
  ON b.comercial_id = dg.comercial_id
 AND b.date_month    = dg.date_month

LEFT JOIN cost_per_lead cpl
  ON b.date_month    = cpl.date_month


