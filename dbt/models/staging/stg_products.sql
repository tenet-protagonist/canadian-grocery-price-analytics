WITH source AS (
    SELECT *
    FROM {{ source('raw', 'PRODUCT') }}
),

cleaned AS (
    SELECT
        -- join key to prices within a day; id regenerates daily
        TRIM(id) AS product_daily_id,

        COALESCE(LOWER(TRIM(vendor)), 'unknown') AS vendor,
        TRIM(product_name) AS product_name,
        NULLIF(TRIM(brand), '') AS brand,
        NULLIF(TRIM(units), '') AS units,
        NULLIF(TRIM(sku), '') AS sku,
        NULLIF(TRIM(upc), '') AS upc,
        TRIM(detail_url) AS detail_url
    FROM source
    WHERE id IS NOT null
       AND vendor IS NOT null
       AND vendor NOT LIKE '%://%'   -- drops URLs
       AND vendor NOT LIKE '%:%'   -- drops timestamps
)

SELECT *
FROM cleaned
