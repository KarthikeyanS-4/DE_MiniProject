
WITH ranked_products AS (
    SELECT
        company,
        product_id,
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
    FROM
        {{ ref('int_tables_combined') }}
    GROUP BY
        company,
        product_id
)

SELECT
    company,
    product_id,
    total_revenue,
    revenue_rank
FROM
    ranked_products
ORDER BY
    revenue_rank