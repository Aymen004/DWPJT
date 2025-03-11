{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stg_reviews AS (
    SELECT DISTINCT
        bank_name,
        agency_name
    FROM {{ ref('stg_reviews') }}
    WHERE agency_name IS NOT NULL
),

-- Join with bank dimension to get bank_id
banks AS (
    SELECT
        bank_id,
        bank_name
    FROM {{ ref('dim_bank') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY s.bank_name, s.agency_name) AS branch_id,
    b.bank_id,
    s.agency_name AS branch_name,
    -- In a real implementation, this might come from the Google Maps API
    MD5(s.bank_name || '-' || s.agency_name) AS google_place_id,
    NOW() AS created_at,
    NOW() AS updated_at
FROM stg_reviews s
JOIN banks b ON s.bank_name = b.bank_name