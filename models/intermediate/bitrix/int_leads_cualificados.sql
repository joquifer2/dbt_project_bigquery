{{ config(
    materialized='table',
    schema='int_bitrix',
    alias='int_leads_cualificados'
) }}

------------------------------------------------------------------------
-- Modelo intermedio: int_leads_cualificados
-- Contactos con deals calificados como válidos (no perdidos, repetidos o fuera de zona).
-- Reutiliza int_contacts, que ya aplica la lógica de join y transformación.
------------------------------------------------------------------------

SELECT
  contact_id,            -- ID único del contacto
  date_create_contact,   -- Fecha de creación del contacto
  postal_code,           -- Código postal del contacto
  deal_id,               -- ID único del deal
  date_create_deal,      -- Fecha de creación del deal
  date_closed_lead,      -- Fecha de cierre del lead
  date_first_visit,      -- Fecha de la primera visita
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
WHERE is_qualified = TRUE


