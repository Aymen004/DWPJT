{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stg_reviews AS (
    SELECT DISTINCT
        location,
        city
    FROM {{ ref('stg_reviews') }}
    WHERE location IS NOT NULL
),

-- Extract district from location (simplified example)
location_enriched AS (
    SELECT
        location,
        city,
        CASE 
            WHEN location ILIKE '%quartier%' THEN SUBSTRING(
                location FROM POSITION('quartier' IN LOWER(location)) + 9 
                FOR POSITION(',' IN SUBSTRING(location FROM POSITION('quartier' IN LOWER(location)) + 9))
            )
            WHEN location ILIKE '%hay%' THEN SUBSTRING(
                location FROM POSITION('hay' IN LOWER(location)) + 4 
                FOR POSITION(',' IN SUBSTRING(location FROM POSITION('hay' IN LOWER(location)) + 4))
            )
            ELSE NULL
        END AS district,
        location AS address,
        NULL::DECIMAL(10,8) AS latitude,
        NULL::DECIMAL(11,8) AS longitude
    FROM stg_reviews
)

SELECT
    ROW_NUMBER() OVER (ORDER BY city, district NULLS LAST) AS location_id,
    city,
    district,
    address,
    latitude,
    longitude,
    NOW() AS created_at,
    NOW() AS updated_at
FROM location_enriched