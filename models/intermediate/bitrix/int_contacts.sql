{{ config(
    materialized='table',
    schema='int_bitrix',
    alias='int_contacts'
) }}

------------------------------------------------------------------------
-- Modelo intermedio: int_contacts
-- Une contactos y deals cuando fueron creados el mismo día.
-- Garantiza que cada contacto tenga como máximo un deal asociado,
-- seleccionando el deal con fecha de cierre más reciente.
------------------------------------------------------------------------

-- CTE: contactos
-- Extrae los datos base de los contactos desde stg_contacts
WITH contactos AS (
  SELECT
    contact_id,           -- ID único del contacto
    date_create_contact,  -- Fecha de creación del contacto (YYYY-MM-DD)
    postal_code           -- Código postal del contacto
  FROM {{ ref('stg_contacts') }}
),

-- CTE: deals_raw
-- Extrae todos los deals válidos, filtrando:
--   - Solo funnels válidos ('0', '17')
--   - Solo si el contacto está presente
--   - Excluye los deals del canal 'UC_YPDXY6' (ampliaciones)
--   - Calcula si el deal está cualificado (no perdido, duplicado o fuera de zona)
deals_raw AS (
  SELECT
    contact_id,           -- ID del contacto asociado
    deal_id,              -- ID único del deal
    date_create_deal,     -- Fecha de creación del deal (YYYY-MM-DD)
    date_closed_lead,     -- Fecha de cierre del lead
    date_first_visit,     -- Fecha de la primera visita
    comercial_id,         -- ID del comercial
    funnel_id,            -- ID del funnel
    stage_id,             -- ID de la etapa
    id_marketing_ghl,     -- ID de marketing GHL
    campaign_name,        -- Nombre de la campaña
    campaign_id,          -- ID de la campaña
    canal_ghl,            -- Canal GHL
    source_id,            -- ID de la fuente
    total_amount,         -- Monto total del deal
    NOT REGEXP_CONTAINS(stage_id, r"C17:LOSE|C17:UC_FBOOU8|C17:UC_K8WTPA") AS is_qualified -- Flag de cualificación
  FROM {{ ref('stg_deals') }}
  WHERE funnel_id IN ('0', '17')
    AND contact_id IS NOT NULL
    AND source_id != 'UC_YPDXY6'
),

-- CTE: deals_ranked
-- Si un contacto tiene más de un deal creado el mismo día,
-- selecciona el que tenga la fecha de cierre más reciente
-- (garantiza unicidad por contacto y fecha de creación)
deals_ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY contact_id, date_create_deal
      ORDER BY date_closed_lead DESC
    ) AS rn
  FROM deals_raw
),

-- CTE: deals
-- Nos quedamos solo con el deal principal por contacto (uno por fecha de creación)
deals AS (
  SELECT *
  FROM deals_ranked
  WHERE rn = 1
)

-- SELECT final
-- Realiza el join principal:
--   - Solo une si el contacto y el deal se crearon el mismo día
--   - Así garantiza un único deal por contacto
SELECT
  c.contact_id,           -- ID único del contacto
  c.date_create_contact,  -- Fecha de creación del contacto
  c.postal_code,          -- Código postal del contacto
  d.deal_id,              -- ID único del deal
  d.date_create_deal,     -- Fecha de creación del deal
  d.date_closed_lead,     -- Fecha de cierre del lead
  d.date_first_visit,     -- Fecha de la primera visita
  d.comercial_id,         -- ID del comercial
  d.funnel_id,            -- ID del funnel
  d.stage_id,             -- ID de la etapa
  d.id_marketing_ghl,     -- ID de marketing GHL
  d.campaign_name,        -- Nombre de la campaña
  d.campaign_id,          -- ID de la campaña
  d.canal_ghl,            -- Canal GHL
  d.source_id,            -- ID de la fuente
  d.total_amount,         -- Monto total del deal
  d.is_qualified          -- Flag de cualificación
FROM contactos c
JOIN deals d
  ON c.contact_id = d.contact_id
  AND c.date_create_contact = d.date_create_deal




