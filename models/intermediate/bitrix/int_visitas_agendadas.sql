{{ config(
    materialized='table',
    alias='int_visitas_agendadas',
    schema='int_bitrix',
    database='datamart-393217',
    meta={'description': 'Visitas agendadas; usa date_create_deal si falta date_first_visit.'}
) }}

------------------------------------------------------------------------
-- Modelo intermedio: int_visitas_agendadas
-- Visitas agendadas a partir de int_contacts, usando date_first_visit o, si falta, date_create_deal.
------------------------------------------------------------------------

SELECT
  COALESCE(date_first_visit, date_create_deal) AS date_first_visit, -- Fecha de la visita agendada (o creación del deal si falta)
  contact_id,            -- ID único del contacto
  date_create_contact,   -- Fecha de creación del contacto
  postal_code,           -- Código postal del contacto
  deal_id,               -- ID único del deal
  date_create_deal,      -- Fecha de creación del deal
  date_closed_lead,      -- Fecha de cierre del lead
  comercial_id,          -- ID del comercial
  funnel_id,             -- ID del funnel
  stage_id,              -- ID de la etapa
  id_marketing_ghl,      -- ID de marketing GHL
  campaign_name,         -- Nombre de la campaña
  campaign_id,           -- ID de la campaña
  canal_ghl,             -- Canal GHL
  source_id,             -- ID de la fuente
  total_amount           -- Monto total del deal
FROM {{ ref('int_contacts') }}
WHERE funnel_id = '0'                         -- Solo funnel principal
  AND NOT REGEXP_CONTAINS(stage_id, r"NEW")  -- Excluye etapas "NEW"
