WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="TO_DATE('2024-02-01')",
        end_date="DATEADD(day, 1, CURRENT_DATE())"
    ) }}
),

dates AS (
    SELECT CAST(date_day AS DATE) AS full_date
    FROM spine
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,     -- for example 20240228
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    DAY(full_date) AS day_of_month,
    DAYNAME(full_date) AS day_name,
    WEEKOFYEAR(full_date) AS week_of_year,
    (DAYNAME(full_date) IN ('Sat', 'Sun')) AS is_weekend
FROM dates
