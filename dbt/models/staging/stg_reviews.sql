{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_reviews AS (
    SELECT
        id,
        bank_name,
        agency_name,
        location,
        review_text,
        rating,
        review_date,
        language,
        raw_data,
        created_at
    FROM {{ source('raw', 'staging_reviews') }}
),

-- Clean and prepare reviews data
prepared_reviews AS (
    SELECT
        id,
        bank_name,
        agency_name,
        location,
        review_text,
        rating,
        CAST(review_date AS DATE) AS review_date,
        COALESCE(language, 'unknown') AS language,
        raw_data,
        created_at,
        
        -- Derive city from location (simple example - would need refinement)
        CASE 
            WHEN location LIKE '%Casablanca%' THEN 'Casablanca'
            WHEN location LIKE '%Rabat%' THEN 'Rabat'
            WHEN location LIKE '%Marrakech%' THEN 'Marrakech'
            WHEN location LIKE '%Fès%' THEN 'Fes'
            WHEN location LIKE '%Tanger%' THEN 'Tangier'
            ELSE 'Other'
        END AS city
    FROM source_reviews
    WHERE review_text IS NOT NULL
)

SELECT
    id AS review_id,
    bank_name,
    agency_name,
    location,
    city,
    review_text,
    rating,
    review_date,
    language,
    raw_data,
    created_at
FROM prepared_reviews