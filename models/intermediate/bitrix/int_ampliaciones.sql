-- models/intermediate/bitrix/int_ampliaciones.sql
-- Modelo intermedio: int_ampliaciones
-- Este modelo filtra y une contactos y deals de Bitrix para identificar ampliaciones específicas según criterios de funnel y source.

{{ config(
    materialized='table',
    schema='int_bitrix',
    alias='int_ampliaciones'
) }}

-- CTE: contactos
-- Selecciona los contactos relevantes con su fecha de creación y código postal
WITH contactos AS (
  SELECT
    contact_id,           -- ID único del contacto
    date_create_contact,  -- Fecha de creación del contacto
    postal_code           -- Código postal del contacto
  FROM {{ ref('stg_contacts') }}
),

-- CTE: deals
-- Selecciona los deals filtrando por funnel_id y source_id específicos
-- Incluye información relevante para el análisis de ampliaciones
deals AS (
  SELECT
    date_create_deal,     -- Fecha de creación del deal
    contact_id,           -- ID del contacto asociado
    deal_id,              -- ID único del deal
    date_closed_lead,     -- Fecha de cierre del lead
    comercial_id,         -- ID del comercial
    funnel_id,            -- ID del funnel
    stage_id,             -- ID de la etapa
    id_marketing_ghl,     -- ID de marketing GHL
    campaign_id,          -- ID de campaña
    canal_ghl,            -- Canal GHL
    source_id,            -- ID de la fuente
    total_amount          -- Monto total del deal
  FROM {{ ref('stg_deals') }}
  WHERE funnel_id IN ('0', '17')
    AND contact_id IS NOT NULL
    AND source_id = 'UC_YPDXY6'
)

-- Selección final: une contactos y deals filtrados
SELECT
  d.date_create_deal,     -- Fecha de creación del deal
  c.contact_id,           -- ID del contacto
  c.date_create_contact,  -- Fecha de creación del contacto
  c.postal_code,          -- Código postal
  d.deal_id,              -- ID del deal
  d.date_closed_lead,     -- Fecha de cierre del lead
  d.comercial_id,         -- ID del comercial
  d.funnel_id,            -- ID del funnel
  d.stage_id,             -- ID de la etapa
  d.source_id,            -- ID de la fuente
  d.total_amount          -- Monto total del deal
FROM contactos c
JOIN deals d
  ON c.contact_id = d.contact_id
