{{ config(materialized='view') }}

-- =============================================================================
-- Modelo: stg_contacts
-- Descripción: Vista staging que estandariza y selecciona los campos clave de los contactos de Bitrix CRM.
-- Incluye el ID del contacto, la fecha de creación y el código postal, todos como STRING.
-- =============================================================================

SELECT 
    CAST(contact_id AS STRING) AS contact_id,           -- ID único del contacto (string)
    EXTRACT(DATE FROM date_create) AS date_create_contact, -- Fecha de creación del contacto (date)
    CAST(address_postal_code AS STRING) AS postal_code     -- Código postal del contacto (string)
FROM `datamart-393217.bitrix_crm.bitrix24crm_contacts`