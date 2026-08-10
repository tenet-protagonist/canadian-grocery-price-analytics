WITH deduplicated AS (
    -- int_products has one row per daily id; collapse to one row per product_key
    -- by keeping a single representative row for each real product
    SELECT *
    FROM {{ ref('int_products') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY product_key ORDER BY product_daily_id DESC) = 1
)

SELECT
    product_key,
    stable_product_id,
    vendor,
    product_name,
    units,
    sku,
    upc,
    upc_normalized,
    -- how product was identified (upc/sku/hash)
    SPLIT_PART(stable_product_id, '|', 2) AS key_tier
FROM deduplicated

UNION ALL

-- every fact row references a real dimension row (no null foreign keys)
SELECT
    '-1' AS product_key,
    'unknown' AS stable_product_id,
    'unknown' AS vendor,
    'Unknown Product' AS product_name,
    NULL AS units,
    NULL AS sku,
    NULL AS upc,
    NULL AS upc_normalized,
    'unknown' AS key_tier
