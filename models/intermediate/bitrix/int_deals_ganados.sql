{{ config(
  materialized='table',
  alias='int_deals_ganados',
  schema='int_bitrix',
  database='datamart-393217',
  meta={'description': 'Deals ganados que han generado una venta'}
) }}

------------------------------------------------------------------------
-- Modelo intermedio: int_deals_ganados
-- Aplica filtro sobre int_contacts para obtener únicamente los deals ganados (stage_id = 'WON').
------------------------------------------------------------------------

SELECT
  date_closed_lead as date_closed_deal,  -- Fecha de cierre del deal (renombrada)
  contact_id,                            -- ID del contacto asociado
  deal_id,                               -- ID único del deal
  funnel_id,                             -- ID del funnel
  stage_id,                              -- ID de la etapa (debe ser 'WON')
  date_create_contact,                   -- Fecha de creación del contacto
  date_create_deal,                      -- Fecha de creación del deal
  date_first_visit,                      -- Fecha de la primera visita
  id_marketing_ghl,                      -- ID de marketing GHL
  canal_ghl,                             -- Canal GHL
  comercial_id,                          -- ID del comercial
  campaign_id,                           -- ID de la campaña
  campaign_name,                         -- Nombre de la campaña
  source_id,                             -- ID de la fuente
  postal_code,                           -- Código postal del contacto
  total_amount                           -- Monto total del deal
FROM {{ ref('int_contacts') }}
WHERE stage_id = 'WON'