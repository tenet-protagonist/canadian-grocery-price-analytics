WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

products AS (
    SELECT product_daily_id, product_key
    FROM {{ ref('int_products') }}
),

joined AS (
    SELECT
        -- foreign keys to the dimensions
        -- no matching product -> point to the '-1' unknown member
        COALESCE(products.product_key, '-1') AS product_key,
        TO_NUMBER(TO_CHAR(prices.observed_at, 'YYYYMMDD')) AS date_key,

        -- degenerate attributes
        prices.observed_at,
        prices.price_format,

        -- measures
        prices.current_price,
        prices.old_price,
        prices.is_on_sale,

        -- "real" price for trend analysis
        (prices.price_format = 'plain_price' AND prices.current_price > 0) AS is_valid_price

    FROM prices
    LEFT JOIN products ON prices.product_daily_id = products.product_daily_id
)

SELECT *
FROM joined
