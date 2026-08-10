WITH resolved AS (
    SELECT
        product_daily_id,
        vendor,
        product_name,
        units,
        sku,
        upc,

        -- normalise the barcode so 12- and 13-digit forms of the same UPC match
        CASE
            WHEN upc IS NOT NULL
                THEN LPAD(upc, 13, '0')
        END AS upc_normalized,

        -- tiered stable identity, scoped within a vendor:
        --   1) UPC where present; 2) vendor + sku; 3) hash of vendor + name + units
        CASE
            WHEN upc IS NOT NULL
                THEN COALESCE(vendor, 'unknown') || '|upc|' || LPAD(upc, 13, '0')
            WHEN sku IS NOT NULL
                THEN COALESCE(vendor, 'unknown') || '|sku|' || sku
            ELSE
                COALESCE(vendor, 'unknown') || '|hash|' || md5(COALESCE(product_name, '') || '|' || COALESCE(units, ''))
        END AS stable_product_id
    FROM {{ ref('stg_products') }}
)

SELECT
    -- surrogate key: one stable id -> one product_key
    {{ dbt_utils.generate_surrogate_key(['stable_product_id']) }} AS product_key,
    product_daily_id,
    stable_product_id,
    upc_normalized,
    vendor,
    product_name,
    units,
    sku,
    upc
FROM resolved
