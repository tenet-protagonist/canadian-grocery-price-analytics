-- Warns (does not fail) when a new vendor appears, it flags without breaking the build
{{ config(severity = 'warn') }}

SELECT
    vendor,
    COUNT(*) AS row_count
FROM {{ ref('stg_products') }}
WHERE vendor NOT IN (
    'loblaws', 'voila', 'nofrills', 'metro',
    'saveonfoods', 'walmart', 'galleria', 'tandt', 'unknown'
)
GROUP BY vendor
