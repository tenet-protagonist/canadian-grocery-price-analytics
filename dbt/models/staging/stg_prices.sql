WITH source AS (
    SELECT * FROM {{ source('raw', 'PRICE') }}
),

typed AS (
    SELECT
        TRIM(product_id) AS product_daily_id,
        TRY_TO_TIMESTAMP(nowtime) AS observed_at,

        -- keep the original strings so nothing is lost
        NULLIF(TRIM(current_price), '') AS current_price_raw,
        NULLIF(TRIM(old_price), '') AS old_price_raw,
        NULLIF(TRIM(price_per_unit), '') AS price_per_unit_raw,
        NULLIF(TRIM(other), '') AS status_note

    FROM source

),

classified AS (
    SELECT
        product_daily_id,
        observed_at,
        current_price_raw,
        old_price_raw,
        price_per_unit_raw,
        status_note,

        -- the price column mixes several formats; label each row.
        -- ~98% are plain prices; the rest are flagged, not dropped.
        CASE
            WHEN current_price_raw ILIKE '%/lb%' OR current_price_raw ILIKE '%/%g%' THEN 'unit_price'
            WHEN current_price_raw ILIKE '%/$%' OR current_price_raw ILIKE '%for%' THEN 'multi_buy'
            WHEN current_price_raw ILIKE '%avg%' OR current_price_raw ILIKE '%ea%' THEN 'estimated'
            WHEN TRY_TO_DECIMAL(REPLACE(REPLACE(current_price_raw, '$', ''), ',', ''), 10, 2) IS NOT NULL THEN 'plain_price'
            ELSE 'other'
        END AS price_format,

        -- clean decimal, non-null only for plain prices (NULL for the rest, by design)
        TRY_TO_DECIMAL(REPLACE(REPLACE(current_price_raw, '$', ''), ',', ''), 10, 2) AS current_price,
        TRY_TO_DECIMAL(REPLACE(REPLACE(old_price_raw, '$', ''), ',', ''), 10, 2) AS old_price,

        -- a struck-through old price means the item wAS on sale
        (old_price_raw IS NOT NULL) AS is_on_sale
    FROM typed

),

deduped AS (
    SELECT *
    FROM classified
    -- the source occASionally lists the same product twice at the same timestamp
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY product_daily_id, observed_at
        ORDER BY current_price nulls LAST
    ) = 1
)

SELECT * FROM deduped
WHERE observed_at IS NOT NULL   -- drop rows whose timestamp couldn't be parsed
