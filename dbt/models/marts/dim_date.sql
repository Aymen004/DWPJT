{{ config(
    materialized='table',
    schema='marts'
) }}

-- Generate date series from 2015 to 2025
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
       )
    }}
),

-- Extract date components
date_with_components AS (
    SELECT
        date_day AS full_date,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
    FROM date_spine
)

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_id,
    full_date,
    day::INTEGER,
    month::INTEGER,
    year::INTEGER,
    quarter::INTEGER,
    day_of_week::INTEGER,
    is_weekend,
    NOW() AS created_at
FROM date_with_components