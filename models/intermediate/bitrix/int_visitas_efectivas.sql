-- models/intermediate/bitrix/int_int_visitas_efectivas.sql
{{ config(
  materialized='table',
  alias='int_visitas_efectivas',
  schema='int_bitrix',
  database='datamart-393217',
  meta={'description': 'Visitas agendadas que se han materializado.Usa date_create_deal si falta date_first_visit.'}
) }}

------------------------------------------------------------------------
-- Modelo intermedio: aplica filtro sobre int_contacts
------------------------------------------------------------------------
SELECT
  COALESCE(date_first_visit, date_create_deal) AS date_first_visit,
  contact_id,
  deal_id,
  funnel_id,
  stage_id,
  date_create_contact,
  date_create_deal,
  id_marketing_ghl,
  canal_ghl,
  comercial_id,
  campaign_id,
  campaign_name,
  source_id,
  postal_code,
  
    
FROM {{ ref('int_contacts') }}
WHERE funnel_id IN ('0') 
  AND NOT REGEXP_CONTAINS(
    stage_id,
    --Excluimos las etapas "gestion pendiente","visitas pendientes","Call center" y perdido, anula la visita"
    r"NEW|PREPAYMENT_INVOICE|UC_OISV41|UC_7LBK41"
  )