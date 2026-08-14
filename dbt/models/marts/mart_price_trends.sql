WITH valid_prices AS (
    -- only real, plain, above-zero prices, joined to their vendor
    SELECT
        p.vendor,
        f.observed_at,
        f.current_price,
        f.is_on_sale
    FROM {{ ref('fct_prices') }} f
    JOIN {{ ref('dim_products') }} p
        ON f.product_key = p.product_key
    WHERE f.is_valid_price
      AND p.vendor <> 'unknown'   -- exclude the orphan bucket
      AND f.current_price < 1000
),

monthly AS (
    SELECT
        vendor,
        DATE_TRUNC('month', observed_at) AS price_month,
        COUNT(*) AS observations,
        MEDIAN(current_price) AS median_price,
        AVG(current_price) AS avg_price,
        MIN(current_price) AS min_price,
        MAX(current_price) AS max_price,
        SUM(CASE WHEN is_on_sale THEN 1 ELSE 0 END) AS on_sale_count,
        ROUND(100.0 * SUM(CASE WHEN is_on_sale THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_on_sale
    FROM valid_prices
    GROUP BY vendor, DATE_TRUNC('month', observed_at)
)

SELECT
    vendor,
    price_month,
    observations,
    median_price,
    avg_price,
    min_price,
    max_price,
    on_sale_count,
    pct_on_sale
FROM monthly
ORDER BY vendor, price_month
