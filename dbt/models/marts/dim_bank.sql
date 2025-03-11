{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stg_reviews AS (
    SELECT DISTINCT
        bank_name
    FROM {{ ref('stg_reviews') }}
),

-- Enrich with bank metadata (in real implementation, this might come from a separate source)
bank_metadata AS (
    SELECT
        bank_name,
        CASE
            WHEN bank_name ILIKE '%Attijariwafa%' THEN 'AWB'
            WHEN bank_name ILIKE '%Bank of Africa%' OR bank_name ILIKE '%BMCE%' THEN 'BOA'
            WHEN bank_name ILIKE '%CIH%' THEN 'CIH'
            WHEN bank_name ILIKE '%Crédit Agricole%' THEN 'CAM'
            WHEN bank_name ILIKE '%Banque Populaire%' THEN 'BP'
            ELSE SUBSTRING(bank_name FROM 1 FOR 3)
        END AS bank_code,
        CASE
            WHEN bank_name ILIKE '%Attijariwafa%' THEN 'Casablanca'
            WHEN bank_name ILIKE '%Bank of Africa%' OR bank_name ILIKE '%BMCE%' THEN 'Casablanca'
            WHEN bank_name ILIKE '%CIH%' THEN 'Casablanca'
            WHEN bank_name ILIKE '%Crédit Agricole%' THEN 'Rabat'
            WHEN bank_name ILIKE '%Banque Populaire%' THEN 'Casablanca'
            ELSE 'Morocco'
        END AS headquarters,
        'Moroccan Bank' AS description
    FROM stg_reviews
)

SELECT
    ROW_NUMBER() OVER (ORDER BY bank_name) AS bank_id,
    bank_name,
    bank_code,
    headquarters,
    description,
    NOW() AS created_at,
    NOW() AS updated_at
FROM bank_metadata